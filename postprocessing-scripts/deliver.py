import sys
import os
import paramiko
import gzip
import bz2
import zlib
import configparser
import log
from datetime import datetime, timedelta
import shutil
import time
import argparse
import processing_functions as pf

import deliver_principals_new
import create_data_dict
import insert_publish_table

logger = log.setup_logging("logging_configs/deliver_log_config.json")

CHUNK_SIZE = 3000 * 1024 * 1024  # 3gb chunk


class Delivery:
    def __init__(self, cred_name, local_path, original_file_name, run_date, config, trigger=False, pre_process_function=None):
        self.cred_name = cred_name
        self.host = config[cred_name]["host"]
        self.username = config[cred_name]["username"]
        self.password = config[cred_name]["password"]
        self.file_name = original_file_name
        self.local_path = local_path
        self.file_extension = config[cred_name]["file_extension"]
        self.remote_path = config[cred_name]["delivery_location"]
        self.port = int(config[cred_name]["port"])
        self.processor = pre_process_function if pre_process_function else None
        self.target_file_name = f"{config[cred_name]['delivery_file_name']}_{run_date}{self.file_extension}"

        if trigger:
            self.trigger_name = f"{config[cred_name]['delivery_file_name']}_{run_date}.TRIGGER"
        else:
            self.trigger_name = None

    def connect_sftp(self):
        try:
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(None, self.username, self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
        except paramiko.ssh_exception.AuthenticationException as auth_exception:
            logger.error(f"Auth exception {auth_exception}")
            raise
        except paramiko.SSHException as ssh_exception:
            logger.error(f"ssh exception: {ssh_exception}")
            raise
        except Exception as e:
            logger.error(f"Error {e}")
            raise
        return sftp

    def transfer_file(self):
        remote_file_path = f"{self.remote_path}/{self.target_file_name}"
        local_file_path = f"{self.local_path}/{self.file_name}"

        logger.info(f"""Processing {self.cred_name} :
            Remote path: {remote_file_path}
            Using processor '{getattr(self.processor, '__name__', 'None')}'
            for {local_file_path}""")

        if not self.processor:
            with self.connect_sftp() as sftp:
                sftp.put(local_file_path, remote_file_path)
        else:
            data_stream = self.processor(local_file_path, remote_file_path)
            with self.connect_sftp() as sftp:
                logger.info(f"Connected to {self.host}")
                with sftp.open(remote_file_path, "wb") as remote_file:
                    remote_file.set_pipelined(True)
                    total_bytes = 0
                    for i, chunk in enumerate(data_stream):
                        if chunk:
                            remote_file.write(chunk)
                            if i % 10 == 0:
                                logger.info(f"Uploading chunk {i} ({len(chunk)} bytes)")

        if self.trigger_name:
            local_trigger_file_path = f"{self.local_path}/{self.trigger_name}"
            remote_trigger_file_path = f"{self.remote_path}/{self.trigger_name}"
            with open(local_trigger_file_path, "w") as f:
                pass

            logger.info(f"Uploading {local_trigger_file_path} to {remote_trigger_file_path}")
            with self.connect_sftp() as sftp:
                sftp.put(local_trigger_file_path, remote_trigger_file_path)

            if os.path.exists(local_trigger_file_path):
                os.remove(local_trigger_file_path)

def bz2_to_gzip(bz2_file, remote_path, chunk_size=CHUNK_SIZE, compression_level=1):
    logger.info(f"{bz2_file=}, {remote_path=}")
    gzip_compressor = zlib.compressobj(level=compression_level,
                                       method=zlib.DEFLATED,
                                       memLevel=5,
                                       wbits=zlib.MAX_WBITS | 16)

    with bz2.open(bz2_file, "rb") as bz2_file:
        while True:
            decompressed_chunk = bz2_file.read(chunk_size)
            if not decompressed_chunk:
                break
            yield gzip_compressor.compress(decompressed_chunk)

    yield gzip_compressor.flush()


def insert_into_reporting_table(table_date, redshift_creds):
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f"call int_staging.insert_into_reporting_table('{table_date}')")


def main(run_date, ocient_deliver=False, generate_social_append_table=False):
    logger.info("Delivery started")
    config = configparser.ConfigParser(interpolation=None)
    config.read("config.ini")
    s3_creds = config["s3_creds"]

    gca_files_location = f"s3://{s3_creds['bucket']}/{s3_creds['gca_records_key']}/"

    create_data_dict.main(["create_data_dict.py", run_date])
    logger.info("Data dictionary created")
    local_path = f"{run_date}/output"
    original_file_name = f"dbh_global_{run_date}.csv.bz2"

    km = Delivery("km", local_path, original_file_name, run_date, config)
    prime = Delivery("prime", local_path, original_file_name, run_date, config, trigger=True)
    ocient = Delivery("ocient-mft", local_path, original_file_name, run_date, config, pre_process_function=bz2_to_gzip)

    delivery_objects = [km, prime]
    if ocient_deliver:
        delivery_objects.append(ocient)
        logger.info("Will deliver to Ocient")

    for delivery_object in delivery_objects:
        try:
            delivery_start_time = time.time()
            delivery_object.transfer_file()
            delivery_end_time = time.time()

            logger.info(f"File transfer time with CHUNK_SIZE = 3gb: {str(timedelta(seconds=(delivery_end_time - delivery_start_time)))}")
        except Exception as e:
            logger.error(f"Failed to process delivery {delivery_object.cred_name} : {e}")

        logger.info(f"Finished delivery for {delivery_object.cred_name}")

    # Deliver principals to ingestion
    deliver_principals_new.main([run_date, generate_social_append_table])
    logger.info("Principals records delivered")

    # Insert into mysql reporting table
    insert_publish_table.main(["insert_publish_table.py", run_date])
    logger.info("MYSQL Metrics updated")

    # Insert into redshift post processing table
    insert_into_reporting_table(run_date, redshift_creds)
    logger.info("Metrics in redshift updated")

    # Clear directory for next run
    #shutil.rmtree(run_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("run_date", type=str, help="Date of the international file to deliver")
    parser.add_argument("--ocient_deliver", action="store_true", help="True or False to deliver to ocient")
    parser.add_argument("--generate_social_append_table", action="store_true", help="Deliver a social appended principal file to ingestion")
    args = parser.parse_args()

    kwargs = {k: v for k, v in vars(args).items() if v is not None}
    SystemExit(main(**kwargs))