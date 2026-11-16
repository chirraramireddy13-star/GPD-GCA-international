import boto3
import os
import sys
import gzip
import shutil
import subprocess
import time
import logging
import csv
from datetime import datetime
import redshift_connector

logging.basicConfig(filename='log/post_processing_log/' + str(datetime.today()) + '.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')

# Too much logging is done by default for aws libraries so set logging to critical
for name in ['boto', 'urllib3', 's3transfer', 'boto3', 'botocore', 'nose']:
    logging.getLogger(name).setLevel(logging.CRITICAL)
logging.getLogger('post_log').setLevel(logging.INFO)
logger = logging.getLogger('post_log')

import intcon_post_new_dict as intcon_post

SAMPLE_LOCATION = 's3://dnb-gpd-international-file/10k-samples/'


def get_files(s3_bucket_key, path):
    make_dir(path)
    os.chdir(path)
    s3 = boto3.resource('s3')
    split_string = s3_bucket_key[5:].split('/')
    bucket, key = split_string[0], split_string[1] + '/' + split_string[2]
    bucket = s3.Bucket(bucket)
    file_objs = list(bucket.objects.filter(Prefix=key))
    for obj in file_objs:
        file_name = obj.key.split('/')[-1]
        if file_name:
            try:
                bucket.download_file(obj.key, file_name)
            except Exception as e:
                logger.error(e)


def unzip_files(path, folder_name):
    files = [f for f in os.listdir(path) if os.path.isfile(f)]
    processes = []
    for f in files:
        outname = f.split('.')[0]
        p = subprocess.Popen('gunzip ' + f, stdout=subprocess.PIPE, shell=True)
        processes.append(p)
    for process in processes:
        process.wait()


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    else:
        for f in [f for f in os.listdir(path) if os.path.isfile(f)]:
            os.remove(f)


def run_post_processing(path, folder_name, orig_dir):
    files = [f for f in os.listdir(path) if os.path.isfile(f)]
    intcon_post.main(files)

def concat_res(path, output_file_name):
    out_path = path + '/output'
    make_dir(out_path)
    files = [f for f in os.listdir(path) if len(f.split('.')) > 2]
    header_written = False
    out_file = out_path + '/' + output_file_name
    with open(out_file, 'w', newline='') as combined_csv:
        writer = csv.writer(combined_csv, dialect='unix')
        for f in files:
            file_ = path + '/' + f
            with open(file_, 'r') as input_file:
                reader = csv.reader(input_file)
                header = next(reader)
                if not header_written:
                    writer.writerow(header)
                    header_written = True
                for line in reader:
                    writer.writerow(line)


def bzip_file(path, output_file_name):
    out_path = path + '/output/'
    os.chdir(out_path)
    cmd = 'bzip2 -z ' + out_path + output_file_name
    logging.info('Running %s', cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    p.wait()


def zip_files(path):
    redshift_dir = path + '/to-redshift'
    make_dir(redshift_dir)
    files = [f for f in os.listdir(path) if len(f.split('.')) > 1]
    processes = []
    for f in files:
        cmd = 'mv ' + path + '/' + f + ' ' + redshift_dir + '/' + f
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        processes.append(p)
    for process in processes:
        process.wait()

    processes = []
    os.chdir(redshift_dir)
    for f in files:
        cmd = 'gzip ' + f
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        processes.append(p)
    for process in processes:
        process.wait()


def create_international_delivered_table(folder_name):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            table_name = 'int_delivered.international_contacts_' + folder_name
            cur.execute("call int_staging.create_delivered_table_new_dict('" + folder_name + "')")
            cur.execute("call int_staging.copy_delivered_table_new_dict('" + folder_name + "')")


def create_samples(folder_name):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            sql = "call int_staging.create_delivered_samples_new_dict('" + folder_name + "', '" + SAMPLE_LOCATION + "')"
            print(sql)
            cur.execute(sql)


def upload_to_s3(s3_bucket_key, path, multi_files=True):
    split_string = s3_bucket_key[5:].split('/')
    if multi_files:
        source_dir = path + '/to-redshift'
        prefix = split_string[1] + '/output'
    else:
        source_dir = path + '/output'
        prefix = split_string[1] + '/file'

    os.chdir(source_dir)
    client = boto3.client('s3')
    bucket = split_string[0]
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for result_file in [f for f in os.listdir(source_dir) if os.path.isfile(f) and ('csv' in f or 'bzip2' in f)]:
        logger.info('uploading %s', result_file)
        try:
            response = client.upload_file(source_dir + '/' + result_file, bucket, prefix + '/' + result_file)
        except Exception as e:
            logger.error(e)


def clear_objects(s3_bucket_key, path, output=False):
    client = boto3.client('s3')
    split_string = s3_bucket_key[5:].split('/')
    bucket = split_string[0]
    if not output:
        prefix = split_string[1] + '/' + split_string[2] + '/'
    else:
        prefix = split_string[1] + '/' + split_string[2].replace('input', 'output') + '/'
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            # delete objects except directory
            if str(obj['Key']) != prefix:
                try:
                    logging.info('Deleting %s', str(obj['Key']))
                    client.delete_object(Bucket=bucket, Key=str(obj['Key']))
                except Exception as e:
                    logger.error(e)


def connect_to_redshift():
    conn = redshift_connector.connect(
        host=os.environ['REDSHIFT_HOST'],
        database=os.environ['REDSHIFT_DATABASE'],
        port=int(os.environ.get('REDSHIFT_PORT', '5439')),
        user=os.environ['REDSHIFT_USER'],
        password=os.environ['REDSHIFT_PASSWORD']
    )
    conn.autocommit = True
    return conn

def copy_from_s3(s3_bucket_key, folder_name):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            cur.execute("call int_staging.export_staging_table_new_dict('" + folder_name + "', '" + s3_bucket_key + "/')")
            logger.info('Exported staging table to redshift')


def insert_into_publish_table(run_date):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            cur.execute("call int_staging.insert_into_reporting_table('" + run_date + "')")


def main(argv):
    s3_bucket_key, folder_name, output_file_name = argv[1], argv[2], argv[3]
    orig_dir = os.getcwd()
    path = os.path.join(os.getcwd(), folder_name)
    logger.info('Beginning run')

    # Clear output directory from previous run
    clear_objects(s3_bucket_key, path, True)
    logger.info('Output object cleared')

    # Move files to S3 from redshift
    copy_from_s3(s3_bucket_key, folder_name)
    logger.info('Files moved to s3')

    # Download files from bucket
    get_files(s3_bucket_key, path)
    os.chdir(path)
    logger.info('Files Downloaded')

    # unzip all downloaded files
    unzip_files(path, folder_name)
    logger.info('Files unzipped')

    # Run post processing script
    run_post_processing(path, folder_name, orig_dir)
    logger.info('Post processing complete')

    # Concatenate pdf's
    concat_res(path, output_file_name)
    logger.info('Output file created')

    # bzip files
    bzip_file(path, output_file_name)
    logger.info('Bzip file created')

    # zip files to send to redshift
    zip_files(path)
    logger.info('Redshift files zipped')

    # upload to s3 output location
    upload_to_s3(s3_bucket_key, path)
    logger.info('Uploaded redshift files')

    # unload files to redshift
    create_international_delivered_table(folder_name)
    logger.info('delivered table created')

    # create samples
    create_samples(folder_name)
    logger.info('samples created')

    # Upload bzip file to redshift
    upload_to_s3(s3_bucket_key, path, False)
    logger.info('Output file uploaded')

    # clear input objects for next run
    clear_objects(s3_bucket_key, path)
    logger.info('Input Objects cleared from directory')

    # clear post processing folder
    shutil.rmtree(path)
    logger.info('Folder: %s', folder_name, ' deleted')

    # insert counts into publish table
    insert_into_publish_table(folder_name)
    logger.info('inserted counts into publish table')


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))