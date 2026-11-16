import boto3
import os
import sys
import gzip
import shutil
import subprocess
import time
import log
import logging
import csv
from datetime import datetime
import redshift_connector
from configparser import ConfigParser
import glob
import export_staging_data

logger = log.setup_logging('logging_configs/post_processing_log_config.json')

import dedupe
import dedupe_by_mrc
import intcon_post
import enforcer as enf

import processing_functions as pf

required_dirs = [
    'log',
    'translate'
]


def download_from_s3(s3_bucket, s3_key, local_dir):
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
    for item in response['Contents']:
        key = item['Key']
        if key.endswith('/'):
            continue
        local_path = os.path.join(local_dir, os.path.relpath(key, s3_key))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        client.download_file(s3_bucket, item['Key'], local_path)


def unzip_files(path):
    files = glob.glob(f'{path}/**/*.gz', recursive=True)
    processes = []

    for f in files:
        p = subprocess.Popen(['gunzip', f])
        processes.append(p)

    for process in processes:
        process.wait()


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    else:
        for f in [f for f in os.listdir(path) if os.path.isfile(f)]:
            os.remove(f)


def run_post_processing(run_date, path):
    files = [f for f in glob.glob(os.path.join(path, '**', '*00')) if os.path.isfile(f)]
    intcon_post.main(run_date, files)


def insert_into_reporting_table(table_date, redshift_creds):
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f"call int_staging.insert_into_reporting_table('{table_date}')")

def main(argv):
    folder_name = argv[1]

    gck_dedupe = False
    if len(argv) > 2:
        gck_dedupe = True

    config = ConfigParser()
    config.read('config.ini')
    s3_key = config['s3_creds']['post_processing_input']
    s3_creds = config['s3_creds']
    s3_bucket = s3_creds['bucket']

    redshift_creds = config['redshift_cluster']

    path = os.path.join(os.getcwd(), folder_name)
    output_path = os.path.join(path, 'output')
    pf.clear_s3_directory(s3_bucket, f'{s3_key}/{folder_name}/')

    if os.path.exists(path):
        shutil.rmtree(path)

    for required_dir in required_dirs:
        os.makedirs(required_dir, exist_ok=True)

    os.makedirs(output_path, exist_ok=True)

    export_staging_data.main(folder_name)
    logger.info('Files moved to S3')
    download_from_s3(s3_bucket, f'{s3_key}/{folder_name}/', path)

    # unzip all downloaded files
    unzip_files(path)
    logger.info('Files Unzipped')

    # Run post processing script
    run_post_processing(folder_name, path)
    logger.info('Post Processing Complete')

    logger.info(output_path)
    file_pattern = '*/*_out.csv'

    if gck_dedupe:
        logger.info('Deduping by gck')
        dedupe.main(path)
        file_pattern = '*/*deduplicated.csv'

    dedupe_by_mrc.main(path, folder_name, file_pattern, output_path)

    logger.info('Processing the files for profanity')
    enf.run_profanity_check(output_path)

    # logger.info(f'Uploading files in {path}/output to {s3_key}/{folder_name}/output')
    # pf.upload_to_s3(s3_bucket, f'{s3_key}/{folder_name}/output', f'{path}/output')

    ## Upload processed files to s3##
    logger.info(f'Uploading files in {path}/output/files_for_delivery to {s3_key}/{folder_name}/output/files_for_delivery')
    pf.upload_to_s3(s3_bucket, f'{s3_key}/{folder_name}/output/files_for_delivery', f'{path}/output/files_for_delivery')

    logger.info(f'Uploading files in {path}/output/files_for_review to {s3_key}/{folder_name}/output/files_for_review')
    pf.upload_to_s3(s3_bucket, f'{s3_key}/{folder_name}/output/files_for_review', f'{path}/output/files_for_review')

    table_name = f'int_delivered.international_contacts_{folder_name}'
    pf.create_international_base_table(redshift_creds, table_name)

    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            copy_command = f"""COPY {table_name}
                FROM 's3://{s3_bucket}/{s3_key}/{folder_name}/output/files_for_delivery'
                IAM_ROLE '{s3_creds['iam_role']}'
                CSV QUOTE '\\"'
                EMPTYASNULL
                FILLRECORD
                IGNOREHEADER 1
                MAXERROR 256
                """
            logger.info(copy_command)
            cur.execute(copy_command)


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))