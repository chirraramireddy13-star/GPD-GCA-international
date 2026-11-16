from google.cloud import translate_v2
import csv
import os
import concurrent.futures
import time
from datetime import datetime
import asyncio
import configparser
import sys
from langdetect import detect, LangDetectException, DetectorFactory, detect_langs
import logging
import MySQLdb
import mrc_classification as mrc
import functools
import contextlib
import requests
sys.path.append('../')
import processing_functions as pf
import log

logger = log.setup_logging('/home/international-execution/post-processing/logging_configs/translate_log_config.json')

MAX_BYTES = 5500000
MAX_REQUEST = 200000
NUM_TITLES = 100000
TRANSLATION_TABLE_NAME = 'int_gen.translations'
CHUNK_SIZE = 25000
MAX_TITLE_BATCH_SIZE = 1999999
MRC_CLASSIFICATION_TABLE = 'int_gen.mrc_classification'
TITLE_UNIQUE_TABLE = 'matchDist.TitleUnique'
MAX_RETRY = 3


async def translate_text(text, title_id, translate_target_language, api_key):
    try:
        url = 'https://translation.googleapis.com/language/translate/v2'
        headers = {'X-goog-api-key': api_key}
        params = {'q': text, 'target': translate_target_language}
        res = requests.post(url, headers=headers, data=params).json()
        translated_text = res['data']['translations'][0]['translatedText']
        detected_source_language = res['data']['translations'][0]['detectedSourceLanguage']
        logger.info(f'title id: {title_id} text: {text}, translated text: {translated_text}, detected language: {detected_source_language}')
        return title_id, text, translated_text, detected_source_language
    except Exception as e:
        logger.error(e)
        return title_id, text, None, None


def insert_new_titles(redshift_creds, run_date):
    query = f'''INSERT INTO int_gen.translations (title, translated_title, country_code, detected_language, add_date, translated_date)
        WITH existing_titles AS (
            SELECT LOWER(raw_title) AS title FROM int_gen.title_unique
            WHERE COALESCE(raw_title, '') <> ''
            UNION
            SELECT LOWER(normal_title) AS title FROM int_gen.title_unique
            WHERE COALESCE(normal_title, '') <> ''
        ),
        most_freq_titles AS (
            SELECT DISTINCT i.job_title AS title,
                i.country_code,
                COUNT(*) AS title_count
            FROM int_staging.international_contacts_{run_date} i
            LEFT JOIN existing_titles e ON LOWER(i.job_title) = e.title
            WHERE COALESCE(e.title, '') = ''
                AND i.primary_mrc_code IS NULL
            GROUP BY i.job_title, i.country_code
            ORDER BY title_count DESC
            LIMIT {NUM_TITLES}
        )
        SELECT title, NULL AS translated_title, country_code,
            NULL AS detected_language, GETDATE() AS add_date,
            NULL AS translated_date
        FROM most_freq_titles'''
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def get_titles_to_translate(redshift_creds, s3_creds):
    logger.info('Getting titles to translate')
    query = f'SELECT id, title FROM {TRANSLATION_TABLE_NAME} WHERE translated_date IS NULL'
    destination = f"{s3_creds['translate_location']}"
    pf.unload_from_redshift(
        redshift_creds, s3_creds, query,
        to_location=f"s3://{s3_creds['bucket']}/{destination}/",
        optional_params=['PARALLEL OFF', "DELIMITER ','", 'HEADER', 'ADDQUOTES']
    )
    logger.info(f"Unloaded to s3://{s3_creds['bucket']}/{destination}/")
    pf.download_from_s3(s3_creds['bucket'], destination, 'translate')
    logger.info('Downloaded to destination')


def is_english(title):
    try:
        lang = detect(title)
        english = (lang == 'en')
        if english:
            logger.info(f'{title} is in english')
            return True
        logger.info(f'{title} is in {lang}')
    except LangDetectException:
        logger.info(f'Cannot detect language - will translate {title}')
    return False


async def process_file(input_file, output_file, api_key, target_language='en-US'):
    current_bytes, current_requests = 0, 0
    tasks = []
    english_titles = []
    with open(input_file, 'rt', newline='\n') as in_file:
        logger.info(f'working on file {input_file}')
        reader = csv.reader(in_file)
        header = next(reader, None)
        with open(output_file, 'w', newline='') as out:
            writer = csv.writer(out, dialect='unix')
            writer.writerow(['id', 'title', 'translated_title', 'detected_language'])
            start_time = time.time()
            logger.info(f'Start time: {start_time}')
            char_count = 0
            for line in reader:
                title_id = line[0]
                title = line[1]
                if not is_english(title):
                    if len(title) + current_bytes > MAX_BYTES or current_requests + 1 > MAX_REQUEST:
                        logger.info(f'Current bytes: {current_bytes} Current requests: {current_requests}')
                        logger.info('Hit max requests and/or max bytes')
                        for task in asyncio.as_completed(tasks):
                            logger.info(task)
                            title_id, raw_title, translated_title, detected_language = await task
                            writer.writerow([title_id, raw_title, translated_title, detected_language])
                        tasks = []
                        elapsed_time = time.time() - start_time
                        logger.info(elapsed_time)
                        remaining_time = 60 - elapsed_time
                        logger.info(f'Time remaining until make more requests: {remaining_time}')
                        if remaining_time > 1:
                            time.sleep(remaining_time)
                        start_time = time.time()
                        current_requests, current_bytes = 0, 0
                    task = asyncio.create_task(translate_text(title, title_id, target_language, api_key))
                    logger.info(f'Created new task {task}')
                    current_bytes += len(title)
                    current_requests += 1
                    logger.info(f'Current bytes: {current_bytes} Current requests: {current_requests}')
                    tasks.append(task)
                else:
                    english_titles.append([title_id, title, title, 'en'])
            for task in asyncio.as_completed(tasks):
                logger.info(task)
                title_id, raw_title, translated_title, detected_language = await task
                writer.writerow([title_id, raw_title, translated_title, detected_language])
            for english_title in english_titles:
                writer.writerow(english_title)


def update_translation_table(redshift_creds, table_name):
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f'''UPDATE {TRANSLATION_TABLE_NAME}
                SET translated_title = t.translated_title,
                    detected_language = t.detected_language,
                    translated_date = GETDATE()
                FROM (SELECT title_id, title, translated_title, detected_language
                    FROM int_gen.translated) t
                WHERE {TRANSLATION_TABLE_NAME}.id = t.title_id''')

def get_new_titles_for_classification(redshift_creds):
    titles_to_insert = list()
    query = f'''INSERT INTO {MRC_CLASSIFICATION_TABLE}
        WITH existing_titles AS (
            SELECT LOWER(raw_title) AS title
            FROM int_gen.title_unique
            WHERE COALESCE(raw_title, '') <> ''
            UNION
            SELECT LOWER(normal_title) AS title
            FROM int_gen.title_unique
            WHERE COALESCE(normal_title, '') <> ''
        )
        SELECT t.title_id, t.title, t.translated_title
        FROM int_gen.translated t
        LEFT JOIN existing_titles e ON LOWER(t.translated_title) = e.title
        LEFT JOIN {MRC_CLASSIFICATION_TABLE} m ON t.title_id = m.title_id
        WHERE COALESCE(e.title, '') = '' AND m.title_id IS NULL'''
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cur.execute(f'SELECT title_id, translated_title FROM {MRC_CLASSIFICATION_TABLE} WHERE classification_sent_date IS NULL')
            for title in cur.fetchall():
                titles_to_insert.append((title[0], title[1]))
            cur.execute(f'''INSERT INTO int_gen.title_unique(raw_title, normal_title, mrc)
                SELECT title, translated_title, 9999 AS mrc
                FROM {MRC_CLASSIFICATION_TABLE}
                WHERE classification_sent_date IS NULL''')
    return titles_to_insert


def retry_on_exception(*allowed_exceptions):
    def wrapper(func):
        @functools.wraps(func)
        def retry(*args, **kwargs):
            wait_sec = 2
            err = None
            for attempt in range(MAX_RETRY):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    is_allowed = False
                    for allowed in allowed_exceptions:
                        if isinstance(e, allowed):
                            raise
                    err = e
                    logger.error(f'Exception in retry loop from {func.__name__}')
                    time.sleep(wait_sec)
            else:
                raise err
        return retry
    return wrapper


@retry_on_exception()
def execute_query(chunk, conn):
    query = '''INSERT IGNORE INTO TitleUnique(title)
        VALUES (%s)'''
    with contextlib.closing(conn.cursor()) as cur:
        cur.executemany(query, chunk)

def get_max_title_id(conn):
    with conn.cursor() as cur:
        query = f'SELECT MAX(titleUnique_id) FROM {TITLE_UNIQUE_TABLE}'
        cur.execute(query)
        return cur.fetchone()[0]


def insert_into_title_unique(sql_creds, titles):
    with MySQLdb.connect(**sql_creds) as conn:
        max_row_before_insert = get_max_title_id(conn)
        logger.info(f'{max_row_before_insert=}')
        chunk = []
        chunk_count = 0
        for title in titles:
            try:
                chunk.append((title.encode('utf-8', 'ignore').decode('utf-8', 'ignore'),))
            except UnicodeEncodeError as err:
                logger.error(err)
                continue
            if len(chunk) >= CHUNK_SIZE:
                execute_query(chunk, conn)
                chunk = []
                chunk_count += 1
        if chunk:
            execute_query(chunk, conn)
            chunk_count += 1
            logger.info(f'{chunk_count} chunks inserted')
        max_row_after_insert = get_max_title_id(conn)
        num_titles_inserted = max_row_after_insert - max_row_before_insert
        logger.info(f'Successfully inserted {num_titles_inserted} records')
        if num_titles_inserted > 0:
            return max_row_before_insert
    return 0


def update_classification_table(redshift_creds, job_guid, batch_id, title_ids):
    update_str = ', '.join(map(str, title_ids))
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            update_query = f'''UPDATE {MRC_CLASSIFICATION_TABLE}
                SET job_guid = '{job_guid}',
                    batch_id = {batch_id},
                    classification_sent_date = GETDATE()
                WHERE title_id IN ({update_str})'''
            logger.info(update_query)
            cur.execute(update_query)


def process_titles(sql_creds, redshift_creds, titles, title_ids):
    max_row_before_insert = insert_into_title_unique(sql_creds, titles)
    if max_row_before_insert:
        job_guid, batch_id = mrc.main([sql_creds, max_row_before_insert])
        logger.info('MRC Classification insert complete')
        update_classification_table(redshift_creds, job_guid, batch_id, title_ids)
        logger.info('Classification table updated')
    else:
        logger.info(f'No titles inserted into {TITLE_UNIQUE_TABLE}')

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    api_key = config['google_translate']['api_key']
    redshift_creds = config['redshift_cluster']
    s3_creds = config['s3_creds']
    bucket = s3_creds['bucket']
    s3_key = s3_creds['translate_location']
    sql_creds = {
        'host': config['mysql_creds']['host'],
        'user': config['mysql_creds']['user'],
        'password': config['mysql_creds']['password'],
        'db': 'matchDist',
        'charset': 'utf8',
        'use_unicode': True,
        'autocommit': True,
        'init_command': 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
    }
    out_dir = 'translate/translated'
    in_dir = 'translate/translations'
    os.makedirs(out_dir, exist_ok=True)
    pf.clear_files(in_dir)
    pf.clear_s3_directory(bucket, s3_key)
    with pf.connect_to_redshift(redshift_creds) as conn:
        run_date = pf.get_last_run_date(conn)
    insert_new_titles(redshift_creds, run_date)
    get_titles_to_translate(redshift_creds, s3_creds)
    input_files = [f for f in os.listdir(in_dir)]
    for input_file in input_files:
        output_file = os.path.join(out_dir, f'translated_titles_{os.path.basename(input_file)}.csv')
        asyncio.run(process_file(f'{in_dir}/{input_file}', output_file, api_key))
    pf.clear_s3_directory(bucket, s3_key)
    pf.upload_to_s3(bucket, f'{s3_key}/translated', out_dir)
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS int_gen.translated;')
            cur.execute('''CREATE TABLE int_gen.translated
                (title_id BIGINT,
                 title VARCHAR(1024),
                 translated_title VARCHAR(1024),
                 detected_language VARCHAR(64)
                );''')
    pf.copy_to_redshift(bucket, s3_key, s3_creds['iam_role'], redshift_creds, 'translated', 'int_gen.translated')
    update_translation_table(redshift_creds, out_dir)
    titles = get_new_titles_for_classification(redshift_creds)
    logger.info(f'Retrieved {len(titles)} new titles for classification')
    title_ids = set()
    titles_chunk = list()
    for title in titles:
        title_ids.add(title[0])
        titles_chunk.append(title[1])
        if len(titles_chunk) >= MAX_TITLE_BATCH_SIZE:
            process_titles(sql_creds, redshift_creds, titles_chunk, title_ids)
            titles_chunk.clear()
            title_ids.clear()
    if titles_chunk:
        process_titles(sql_creds, redshift_creds, titles_chunk, title_ids)


if __name__ == '__main__':
    raise SystemExit(main())