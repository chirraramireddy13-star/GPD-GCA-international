import redshift_connector
import paramiko
import boto3
import os
from datetime import datetime, date, timedelta
import subprocess
import logging
import xxhash
from suppressions import BYTE_SUPPRESSIONS
import emoji
import re
from configparser import ConfigParser
from stat import S_ISDIR
import diacritical_replace

logger = logging.getLogger(__name__)

table_names = {
    'orb': 'int_sources.orb',
    'thomson_reuters': 'int_sources.thomson_reuters',
    'override_workstation_delete': 'int_sources.override_workstation_deletes',
    'override_workstation_add': 'int_sources.override_workstation',
    'override_workstation_change': 'int_sources.override_workstation_changes',
    'tracking_table': 'int_gen.table_load_tracking',
}

SPECIAL_CHARACTERS = '!@#$%^&*()+-=_<>/;:,."'
DIGITS = '0123456789'
BUFFER_SIZE = 1024 * 1024 * 4

config = ConfigParser()
config.read('config.ini')
if config['env']['dev']:
    jump_host = config['jump_host']['host']
    jump_host_user = config['jump_host']['username']
    jump_host_password = config['jump_host']['password']


def get_intcon_header():
    return [
        'global_contact_key', 'full_name', 'first_name', 'middle_name', 'last_name', 'name_prefix', 'name_suffix',
        'job_title', 'country_code', 'duns_id', 'duns', 'domestic_ultimate_duns_id', 'global_ultimate_duns_id',
        'primary_phone', 'primary_phone_region_format', 'primary_phone_extension',
        'primary_mrc_code_description', 'ceo_indicator', 'email', 'email_deliverability', 'primary_phone_type',
        'secondary_phone_type', 'secondary_phone', 'secondary_phone_region_format', 'secondary_phone_extension',
        'primary_phone', 'linkedin_profile_url', 'linkedin_profile_url',
        'phone_accuracy_score', 'social_verified_flag', 'twitter_profile_url', 'facebook_profile_url',
        'contact_suitability_flags', 'contact_suitability_flags_string', 'company_name', 'data_source_id',
        'instagram_profile_url', 'gca_contact_reachability_score', 'smart_sort_score',
        'email_source_id', 'phone_source_id', 'source_unique_id', 'suppressed', 'match_key', 'contact_id', 'email_presence_indicator',
        'email_domain', 'gca_owner_id', 'gca_title_match_flag', 'gca_validity', 'gca_vanity_title',
        'individual_id', 'role_ids', 'row_num', 'gca_verification_timestamp',
    ]


def get_nicknames(redshift_creds):
    nicknames = dict()
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT LOWER(nickname), LOWER(first_name) FROM int_gen.nickname_to_first_name WHERE best_nickname_flag = 1')
            for row in cur.fetchall():
                nicknames[row[0]] = row[1]
    return nicknames

def create_match_key(first_name, last_name, domestic_ultimate_duns_id, nicknames):
    first_name_from_nickname = nicknames.get(first_name.strip(), first_name.strip())
    full_name = set_full_name(first_name_from_nickname, None, last_name).lower()
    full_name_for_match_key = diacritical_replace.replace_name(full_name)
    return get_global_contact_key(full_name_for_match_key, domestic_ultimate_duns_id, None)


def parse_name_for_match_key(full_name, nicknames):
    name_split = full_name.split()
    first_name = nicknames.get(name_split[0], name_split[0])
    last_name = name_split[-1] if len(name_split) > 1 else ''
    return first_name, last_name


def fetch_wb_data(duns_ids, redshift_creds):
    batch_str = ','.join(f'{num}' for num in duns_ids)
    duns_map = dict()
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            query = f'''SELECT wb.duns_id, wb.iso2_country_code, wb.site_marketability_flag, wb.business_name, wb.domestic_ultimate_duns_id, wb.global_ultimate_duns_id
                        FROM gsr17.int_gen.worldbase wb
                        WHERE wb.duns_id IN ({batch_str})'''
            cur.execute(query)
            for wb_record in cur.fetchall():
                duns_map[int(wb_record[0])] = {
                    'iso2_country_code': wb_record[1],
                    'site_marketability_flag': wb_record[2],
                    'business_name': wb_record[3],
                    'domestic_ultimate_duns_id': wb_record[4],
                    'global_ultimate_duns_id': wb_record[5],
                }
    return duns_map


def set_full_name(first_name, middle_name, last_name):
    full_name = ' '.join(filter(None, [
        (first_name or '').strip(),
        (middle_name or '').strip(),
        (last_name or '').strip(),
    ]))
    return full_name


def create_global_contact_key(full_name, duns_id, suffix):
    result = int(duns_id) << 32
    name_data = '{};{}'.format((full_name or '').lower(), (suffix or '').lower()).lower()
    h = xxhash.xxh32(name_data)
    result |= h.intdigest()
    return result


def contains_emoji(value):
    return any(emoji.is_emoji(char) for char in value)


def has_digit(value):
    if value in DIGITS:
        return True


def has_three_consecutive_special_chars(value):
    count = 0
    for char in value:
        if char in SPECIAL_CHARACTERS or char.encode('utf-8') in BYTE_SUPPRESSIONS:
            count += 1
            if count >= 3:
                return True
        else:
            count = 0

def is_valid_name(name):
    if name:
        lower_name = name.lower()
        if lower_name.isalpha():
            return True
        elif has_three_consecutive_special_chars(lower_name):
            return False
        elif any(has_digit(char) for char in lower_name):
            return False
        elif contains_emoji(lower_name):
            return False
        elif any(char.encode('utf-8') in BYTE_SUPPRESSIONS for char in lower_name):
            return False
        return True
    return False


def get_file(creds, remote_path, file_name, local_path, dev=False):
    command = f'''lftp sftp://{creds['username']}@{creds['host']}:2222 --password {creds['password']} -e 'get {remote_path}/{file_name} -o {local_path}/{file_name}; bye' '''
    logger.info(command)
    if dev:
        try:
            command = f'get {remote_path}/{file_name} {local_path}/{file_name} bye'
            subprocess.run(['sftp', '-v', 'sftp-through-prod'], input=command, text=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f'SFTP Tunnel failed {e}')
    else:
        p = subprocess.Popen(command, shell=True)
        p.wait()


def get_latest_file(sftp_creds, sftp_path, local_path, pattern=None):
    logger.info(pattern)
    latest = 0
    latest_file_name = None
    with sftp_connect(sftp_creds, True) as sftp:
        for file_attr in sftp.listdir_attr(sftp_path):
            if pattern and pattern.fullmatch(file_attr.filename) and file_attr.st_mtime > latest:
                latest = file_attr.st_mtime
                logger.info(f'{latest}, {datetime.fromtimestamp(file_attr.st_mtime)}')
                latest_file_name = file_attr.filename
            elif not pattern:
                if file_attr.st_mtime > latest:
                    latest = file_attr.st_mtime
                    latest_file_name = file_attr.filename
    if latest_file_name is not None:
        return latest_file_name
    return None


def get_new_data(sftp, last_loaded_file_date, remote_path, local_dir, pattern):
    new_files = False
    file_list = sftp.listdir_attr(f'{remote_path}/')
    for item in file_list:
        logger.info(item)
        mode = item.st_mode
        if not S_ISDIR(mode):
            try:
                # Extract YYYYMMDD from filename to compare with last run date
                file_date = str(re.search('(20\\d{2})(\\d{2})(\\d{2})', item.filename).group())
            except Exception:
                continue
            if pattern.fullmatch(item.filename) and file_date > last_loaded_file_date:
                logger.info(f'Getting new data: {item.filename}')
                remote_file_name = os.path.join(remote_path, item.filename)
                local_file_name = os.path.join(local_dir, item.filename)
                with sftp.open(remote_file_name, 'rb') as remote_file:
                    with open(local_file_name, 'wb') as local_file:
                        while True:
                            data_chunk = remote_file.read(BUFFER_SIZE)
                            if not data_chunk:
                                break
                            local_file.write(data_chunk)
                new_files = True
    return new_files

def connect_to_redshift(creds):
    conn = redshift_connector.connect(
        host=creds['host'],
        database=creds['database'],
        port=creds['port'],
        user=creds['user'],
        password=creds['password'],
    )
    conn.autocommit = True
    return conn


def get_last_run_date(conn):
    logging.info('in get run date')
    with conn.cursor() as cur:
        cur.execute('SELECT date(max(countdate)) from int_metrics.intl_publish_counts')
        run_date = str(cur.fetchone()[0]).replace('-', '').replace(':', '')
    return run_date


def sftp_connect(creds, dev=False):
    try:
        if dev:
            jump_client = paramiko.SSHClient()
            jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            jump_client.connect(str(jump_host), username=str(jump_host_user), password=str(jump_host_password), timeout=360)
            transport = jump_client.get_transport()
            if transport:
                transport.set_keepalive(30)
            channel = transport.open_channel('direct-tcpip', (creds['host'], 2222), ('127.0.0.1', 0))
            sftp_transport = paramiko.Transport(channel)
            sftp_transport.connect(None, creds['username'], creds['password'])
        else:
            sftp_transport = paramiko.Transport((creds['host'], 2222))
            sftp_transport.connect(None, creds['username'], creds['password'])
        sftp_transport.set_keepalive(30)
        sftp = paramiko.SFTPClient.from_transport(sftp_transport)
    except Exception as e:
        logging.error(f'Error {e}')
        raise
    return sftp

def upload_to_s3(bucket, s3_location, path):
    client = boto3.client('s3')
    for result_file in [f for f in os.listdir(path) if 'DS_Store' not in f]:
        upload_file = os.path.join(path, result_file)
        response = client.upload_file(upload_file, bucket, f'{s3_location}/{result_file}')


def truncate_table(redshift_creds, table_name):
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE {table_name}')


def check_file_exists(creds, remote_path, file_name):
    command = f'''lftp sftp://{creds['username']}@{creds['host']}:2222 --password {creds['password']} -e 'ls {remote_path}/{file_name}; exit' '''
    try:
        p = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
        return True
    except subprocess.CalledProcessError as e:
        if 'No such file' in str(e.output):
            return False


def update_country_code(table_name, redshift_creds):
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE {table_name} ADD COLUMN country_code CHAR(2)')
            cur.execute(f'''UPDATE {table_name}
                SET country_code = ws.iso2_country_code
                FROM (SELECT duns_id, iso2_country_code FROM int_gen.worldbase) ws
                WHERE {table_name}.duns_id = ws.duns_id''')


def update_gck(table_name, redshift_creds):
    full_name_string = "NVL(first_name, '') || RTRIM(' ' || NVL(middle_name, '')) || RTRIM(' ' || NVL(last_name, ''))"
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE {table_name} ADD COLUMN duns_id INT')
            cur.execute(f'UPDATE {table_name} SET duns_id = duns::INT')
            cur.execute(f'ALTER TABLE {table_name} ADD COLUMN global_contact_key BIGINT')
            cur.execute(f'''UPDATE {table_name}
                SET global_contact_key = SPID(duns_id, {full_name_string}, name_suffix)''')

def copy_to_redshift(bucket, s3_location, iam_role, redshift_creds, file_type, table_name, file_name=None, delimiter=',', gzip=False, params=None):
    if not file_name:
        from_string = f's3://{bucket}/{s3_location}/'
    else:
        from_string = f's3://{bucket}/{s3_location}/{file_name}'
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            if file_type != 'orb':
                query = f'''COPY {table_name}
                    FROM '{from_string}'
                    IAM_ROLE '{iam_role}'
                    DELIMITER '{delimiter}'
                    DATEFORMAT 'auto'
                    FILLRECORD
                    EMPTYASNULL
                    CSV QUOTE '"'
                    IGNOREHEADER 1 '''
            else:
                query = f'''COPY {table_name}
                    (duns,
                    business_name,
                    domain,
                    first_name,
                    middle_name,
                    last_name,
                    name_suffix,
                    full_name,
                    title,
                    email,
                    phone,
                    linkedin,
                    twitter,
                    url,
                    scalar_score,
                    monitoring_result)
                    FROM '{from_string}'
                    IAM_ROLE '{iam_role}'
                    DELIMITER '{delimiter}'
                    DATEFORMAT 'auto'
                    FILLRECORD
                    CSV QUOTE '"'
                    EMPTYASNULL
                    IGNOREHEADER 1 '''
            if gzip:
                query += 'GZIP'
            if params:
                for param in params:
                    query += param
            logger.info(query)
            cur.execute(query)


def update_tracking_table(redshift_creds, file_name, table_name, data_source_id):
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f'''INSERT INTO {table_names.get('tracking_table')}
                VALUES ('{file_name}', '{table_name}', '{datetime.now()}', {data_source_id})''')


def rotate_table(redshift_creds, source_table_name, new_table_name, data_source_id):
    logger.info('in rotate')
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT file_name, table_name, date(date_loaded::varchar) FROM int_gen.table_load_tracking WHERE data_source_id = {data_source_id} ORDER BY date_loaded DESC LIMIT 1')
            data = cur.fetchone()
            backup_table_name = f'{str(data[1]).split(".")[1]}_{str(data[2]).replace("-", "")}'
            logger.info(backup_table_name)
            cur.execute(f'CREATE TABLE IF NOT EXISTS old_int_sources.{backup_table_name} (LIKE {source_table_name})')
            query = f'INSERT INTO old_int_sources.{backup_table_name} SELECT * FROM {source_table_name}'
            logger.info(query)
            cur.execute(query)
            cur.execute(f'DROP TABLE {source_table_name}')
            query = f'ALTER TABLE {new_table_name} RENAME TO {source_table_name.split(".")[1]}'
            logger.info(query)
            cur.execute(query)

def check_date_last_file(redshift_creds, data_source_id, table_name):
    logger.info('in check')
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            query = f'''SELECT date(MAX(date_loaded)) FROM int_gen.table_load_tracking
                WHERE data_source_id = {data_source_id}
                AND table_name like '{table_name}%' '''
            cur.execute(query)
            latest_file_date = str(cur.fetchone()[0]).replace('-', '')
    logger.info(f'{latest_file_date=}')
    return latest_file_date


def get_global_contact_key(full_name, duns_id, suffix):
    result = int(duns_id) << 32
    name_data = '{};{}'.format((full_name or '').lower(), (suffix or '').lower()).lower()
    h = xxhash.xxh32(name_data)
    result |= h.intdigest()
    return result


def unload_from_redshift(redshift_creds, s3_creds, select_query, to_location, optional_params=[]):
    query = f'''UNLOAD
        ('{select_query}')
        TO '{to_location}'
        IAM_ROLE '{s3_creds["iam_role"]}'
        ALLOWOVERWRITE \n '''
    for param in optional_params:
        query += param
        query += '\n'
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def download_from_s3(s3_bucket, s3_key, local_dir):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    objs = list(bucket.objects.filter(Prefix=s3_key))
    for obj in objs:
        if obj.key.endswith('/'):
            continue
        bucket.download_file(obj.key, obj.key)


def clear_files(local_dir):
    for local_file in [f for f in os.listdir(local_dir)]:
        os.unlink(os.path.join(local_dir, local_file))


def clear_s3_directory(s3_bucket, s3_key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    objs = list(bucket.objects.filter(Prefix=s3_key))
    for obj in objs:
        if not obj.key.endswith('/'):
            obj.delete()

def create_international_base_table(redshift_creds, table_name):
    with connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(f"""CREATE TABLE {table_name} (
                global_contact_key INT8 DISTKEY,
                full_name VARCHAR(1024),
                first_name VARCHAR(1024),
                middle_name VARCHAR(1024),
                last_name VARCHAR(1024),
                name_prefix VARCHAR(65535),
                name_suffix VARCHAR(65535),
                job_title VARCHAR(5024),
                country_code CHAR(2),
                duns_id INT4,
                duns CHAR(9),
                domestic_ultimate_duns_id INT4,
                global_ultimate_duns_id INT4,
                primary_mrc_code CHAR(4),
                mrc_codes VARCHAR,
                primary_mrc_code_description VARCHAR(1024),
                ceo_indicator boolean,
                email VARCHAR(1024),
                email_deliverability INT4,
                primary_phone VARCHAR(256),
                primary_phone_region_format VARCHAR(256),
                primary_phone_extension VARCHAR(256),
                secondary_phone VARCHAR(256),
                secondary_phone_region_format VARCHAR(256),
                secondary_phone_extension VARCHAR(256),
                primary_phone_type VARCHAR(256),
                secondary_phone_type VARCHAR(256),
                phone_accuracy_score INT4,
                social_verified_flag BOOLEAN,
                twitter_profile_url VARCHAR(1024),
                facebook_profile_url VARCHAR(1024),
                linkedin_profile_url VARCHAR(1024),
                instagram_profile_url VARCHAR(1024),
                gca_contact_reachability_score INT4,
                smart_sort_score INT4,
                contact_suitability_flags INT4,
                contact_suitability_flags_string VARCHAR(1024),
                company_name VARCHAR(1024),
                data_source_id INT4,
                email_source_id INT4,
                phone_source_id INT4,
                source_unique_id VARCHAR(1024),
                suppressed boolean DEFAULT false,
                match_key bigint,
                contact_id bigint DEFAULT NULL,
                email_presence_indicator CHAR(2) DEFAULT NULL,
                email_domain VARCHAR(95) DEFAULT NULL,
                gca_owner_id INT4 DEFAULT NULL,
                gca_title_match_flag SMALLINT DEFAULT NULL,
                gca_validity INT4 DEFAULT NULL,
                gca_vanity_title VARCHAR(200) DEFAULT NULL,
                individual_id INT8 DEFAULT NULL,
                role_ids VARCHAR(255) DEFAULT NULL,
                row_num INT8 DEFAULT NULL,
                gcm_verification_timestamp TIMESTAMP DEFAULT NULL
            )""")