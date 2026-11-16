import processing_functions as pf
import sys
from configparser import ConfigParser
import paramiko
import os
import shutil
import logging

logger = logging.getLogger(__name__)


def create_principals_gca_table(redshift_creds, run_date, social_data_append_table):
    table_name = f"int_staging.principal_contacts_gca_markets_{run_date}"
    if social_data_append_table:
        columns = [
            'global_contact_key BIGINT DISTKEY', 'full_name VARCHAR(1024)',
            'first_name VARCHAR(1024)', 'middle_name VARCHAR(1024)',
            'last_name VARCHAR(1024)', 'name_prefix VARCHAR(1024)',
            'name_suffix VARCHAR(1024)', 'job_title VARCHAR(1024)',
            'country_code CHAR(2)', 'duns_id INTEGER', 'duns CHAR(9)',
            'primary_mrc_code CHAR(4)', 'mrc_codes VARCHAR(1024)',
            'primary_mrc_code_description VARCHAR(1024)', 'ceo_indicator BOOLEAN',
            'intl_email_populated BOOLEAN', 'intl_phone_populated BOOLEAN',
            'email VARCHAR(1024)', 'primary_phone VARCHAR(1024)',
            'secondary_phone VARCHAR(1024)', 'social_verified_flag BOOLEAN',
            'twitter_profile_url VARCHAR(1024)', 'facebook_profile_url VARCHAR(1024)',
            'linkedin_profile_url VARCHAR(1024)', 'smart_sort_score INTEGER',
            'contact_suitability_flags INTEGER',
            'contact_suitability_flags_string VARCHAR(1024)',
            'company_name VARCHAR(1024)', 'phone1_ownerid INTEGER',
            'phone2_ownerid INTEGER', 'email_ownerid INTEGER',
            'phone1_originid INTEGER', 'phone2_originid INTEGER',
            'email_originid INTEGER', 'linkedin_originid VARCHAR(256)',
            'facebook_originid VARCHAR(256)', 'twitter_originid VARCHAR(256)',
            'contact_id BIGINT'
        ]
    else:
        columns = [
            'global_contact_key BIGINT DISTKEY', 'full_name VARCHAR(1024)',
            'first_name VARCHAR(1024)', 'middle_name VARCHAR(1024)',
            'last_name VARCHAR(1024)', 'name_prefix VARCHAR(1024)',
            'name_suffix VARCHAR(1024)', 'job_title VARCHAR(1024)',
            'country_code CHAR(2)', 'duns_id INTEGER', 'duns CHAR(9)',
            'primary_mrc_code CHAR(4)', 'mrc_codes VARCHAR(1024)',
            'primary_mrc_code_description VARCHAR(1024)', 'ceo_indicator BOOLEAN',
            'email VARCHAR(1024)', 'email_deliverability int',
            'primary_phone VARCHAR(1024)', 'primary_phone_region_format VARCHAR(128)',
            'primary_phone_extension VARCHAR(128)', 'secondary_phone VARCHAR(1024)',
            'secondary_phone_region_format VARCHAR(128)',
            'secondary_phone_extension VARCHAR(128)',
            'primary_phone_type VARCHAR(128)', 'secondary_phone_type VARCHAR(128)',
            'phone_accuracy_score INTEGER', 'social_verified_flag BOOLEAN',
            'twitter_profile_url VARCHAR(256)', 'facebook_profile_url VARCHAR(256)',
            'linkedin_profile_url VARCHAR(256)', 'smart_sort_score INTEGER',
            'contact_suitability_flags INTEGER',
            'contact_suitability_flags_string VARCHAR(1024)',
            'company_name VARCHAR(1024)'
        ]
    query = f"CREATE TABLE {table_name} (\n" + ',\n'.join(columns) + ");"
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(query)
    logger.info('GCA Principals table created')
    return table_name

def get_latest_gca_table_name(cur):
    cur.execute("""SELECT table_name,
        split_part(table_name, '_', 2) as file_date
        FROM svv_all_tables
        WHERE database_name = 'gca'
          AND table_name like 'globalcontactauthority_2%'
          AND table_name not like '%_load'
        ORDER BY file_date desc
        LIMIT 1""")
    return cur.fetchone()[0]


def populate_table(redshift_creds, table_name, run_date, social_data_append_table):
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            latest_gca_table_name = get_latest_gca_table_name(cur)
            if social_data_append_table:
                # Keep the select-list order aligned with the social-append table schema.
                select_columns = [
                    'p.global_contact_key', 'p.full_name', 'p.first_name',
                    'p.middle_name', 'p.last_name', 'p.name_prefix', 'p.name_suffix',
                    'p.job_title', 'p.country_code', 'p.duns_id', 'p.duns',
                    'p.primary_mrc_code', 'p.mrc_codes',
                    'p.primary_mrc_code_description', 'p.ceo_indicator',
                    "CASE WHEN COALESCE(p.email, '') <> '' THEN True ELSE False END AS intl_email_populated",
                    "CASE WHEN COALESCE(p.primary_phone, '') <> '' THEN True ELSE False END AS intl_phone_populated",
                    'CASE WHEN intl_email_populated THEN p.email ELSE g.email END AS email',
                    'CASE WHEN intl_phone_populated THEN p.primary_phone ELSE g.primaryphone END AS primary_phone',
                    'g.secondaryphone AS secondary_phone', 'p.social_verified_flag',
                    'g.twitterprofileurl AS twitter_profile_url',
                    'g.facebookprofileurl AS facebook_profile_url',
                    'g.linkedinprofileurl AS linkedin_profile_url',
                    'p.smart_sort_score', 'p.contact_suitability_flags',
                    'p.contact_suitability_flags_string', 'p.company_name',
                ]
                phone_owner_case = """CASE
                    WHEN intl_phone_populated AND country_code = 'US' THEN 5593
                    WHEN intl_phone_populated AND country_code = 'GB' THEN 5594
                    WHEN intl_phone_populated AND country_code = 'IE' THEN 5595
                    WHEN intl_phone_populated AND country_code = 'IN' THEN 5596
                    WHEN intl_phone_populated AND country_code = 'CA' THEN 5597
                    WHEN intl_phone_populated IS False AND COALESCE(g.primaryphone, '') <> '' THEN g.phone1_ownerid
                    ELSE NULL END"""
                email_owner_case = """CASE
                    WHEN intl_email_populated AND country_code = 'US' THEN 5593
                    WHEN intl_email_populated AND country_code = 'GB' THEN 5594
                    WHEN intl_email_populated AND country_code = 'IE' THEN 5595
                    WHEN intl_email_populated AND country_code = 'IN' THEN 5596
                    WHEN intl_email_populated AND country_code = 'CA' THEN 5597
                    WHEN intl_email_populated IS False AND COALESCE(g.email, '') <> '' THEN g.email_ownerid
                    ELSE NULL END"""
                select_columns.extend([
                    f'{phone_owner_case} AS phone1_ownerid',
                    'g.phone2_ownerid', f'{email_owner_case} AS email_ownerid',
                    'g.phone1_originid', 'g.phone2_originid', 'g.email_originid',
                    'g.linkedin_originid', 'g.facebook_originid',
                    'g.twitter_originid', 'g.contact_id'
                ])
                query = (f"INSERT INTO {table_name}\nSELECT " + ',\n'.join(select_columns)
                         + f"\nFROM int_staging.principal_contacts_{run_date} p\n"
                         + f"LEFT JOIN gca.public.{latest_gca_table_name} g "
                           "ON p.global_contact_key = g.global_contact_key\n"
                           "AND LOWER(p.job_title) = LOWER(g.jobtitle)\n"
                           "WHERE p.country_code in ('GB','US','CA','IN','IE')\n"
                           "AND p.suppressed IS NOT True")
            else:
                fields = [
                    'global_contact_key', 'full_name', 'first_name', 'middle_name',
                    'last_name', 'name_prefix', 'name_suffix', 'job_title',
                    'country_code', 'duns_id', 'duns', 'primary_mrc_code',
                    'mrc_codes', 'primary_mrc_code_description',
                    'ceo_indicator::int2', 'email', 'email_deliverability',
                    'primary_phone', 'primary_phone_region_format',
                    'primary_phone_extension', 'secondary_phone',
                    'secondary_phone_region_format', 'secondary_phone_extension',
                    'primary_phone_type', 'secondary_phone_type',
                    'phone_accuracy_score', 'social_verified_flag::int2',
                    'twitter_profile_url', 'facebook_profile_url',
                    'linkedin_profile_url', 'smart_sort_score',
                    'contact_suitability_flags', 'contact_suitability_flags_string',
                    'company_name'
                ]
                query = (f"INSERT INTO {table_name}\nSELECT " + ',\n'.join('p.' + x for x in fields)
                         + f"\nFROM int_staging.principal_contacts_{run_date} p\n"
                           "WHERE p.country_code in ('GB','US','CA','IN','IE')\n"
                           "AND p.suppressed IS NOT True")
            logger.info(f'{query=}')
            cur.execute(query)
            logger.info(f'Inserted all data into {table_name}')

def transfer_file(s3_creds, sftp_creds, local_path):
    file_dict = {}
    with pf.sftp_connect(sftp_creds) as sftp:
        for f in [f for f in os.listdir(local_path) if 'dbh' in f]:
            country_folder = f.split('_')[1]
            logger.info(f'Moving {local_path}/{f} to global_contacts/{country_folder}/{f}')
            sftp.put(f'{local_path}/{f}', f'global_contacts/{country_folder}/{f}')


def unload_from_redshift(redshift_creds, s3_creds, table_name, run_date, social_data_append_table):
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            for market in ['GB', 'CA', 'IE', 'IN', 'US']:
                file_name = 'uk' if market == 'GB' else market.lower()
                if social_data_append_table:
                    fields = [
                        'global_contact_key', 'full_name', 'first_name', 'middle_name',
                        'last_name', 'name_prefix', 'name_suffix', 'job_title',
                        'country_code', 'duns_id', 'duns', 'primary_mrc_code',
                        'mrc_codes', 'primary_mrc_code_description',
                        'ceo_indicator::int2', 'email', 'primary_phone',
                        'secondary_phone', 'social_verified_flag::int2',
                        'twitter_profile_url', 'facebook_profile_url',
                        'linkedin_profile_url', 'smart_sort_score',
                        'contact_suitability_flags', 'contact_suitability_flags_string',
                        'company_name', 'phone1_ownerid', 'phone2_ownerid',
                        'email_ownerid', 'phone1_originid', 'phone2_originid',
                        'email_originid', 'linkedin_originid',
                        'facebook_originid', 'twitter_originid', 'contact_id'
                    ]
                else:
                    fields = [
                        'global_contact_key', 'full_name', 'first_name', 'middle_name',
                        'last_name', 'name_prefix', 'name_suffix', 'job_title',
                        'country_code', 'duns_id', 'duns', 'primary_mrc_code',
                        'mrc_codes', 'primary_mrc_code_description',
                        'ceo_indicator::int2', 'email', 'email_deliverability',
                        'primary_phone', 'primary_phone_region_format',
                        'primary_phone_extension', 'secondary_phone',
                        'secondary_phone_region_format', 'secondary_phone_extension',
                        'primary_phone_type', 'secondary_phone_type',
                        'phone_accuracy_score', 'social_verified_flag::int2',
                        'twitter_profile_url', 'facebook_profile_url',
                        'linkedin_profile_url', 'smart_sort_score',
                        'contact_suitability_flags', 'contact_suitability_flags_string',
                        'company_name'
                    ]
                select_fields = ',\n'.join(fields)
                unload_query = f"""UNLOAD ('
                    SELECT {select_fields}
                    FROM {table_name} WHERE country_code = ''{market}'' ')
                    TO 's3://{s3_creds['bucket']}/{s3_creds['gca_records_key']}/{run_date}/dbh_{file_name}_{run_date}'
                    IAM_ROLE '{s3_creds['iam_role']}'
                    FORMAT AS CSV
                    CLEANPATH
                    HEADER
                    GZIP
                    PARALLEL OFF"""
                logger.info(f'{unload_query=}')
                cur.execute(unload_query)

def main(argv):
    config = ConfigParser()
    config.read('config.ini')
    run_date = argv[0]
    social_data_append_table = argv[1]
    if social_data_append_table:
        logger.info('Creating social append table')
    else:
        logger.info('Creating default principals table')
    redshift_creds = config['redshift_cluster']
    s3_creds = config['s3_creds']
    sftp_creds = config['km']
    s3_key = f"{s3_creds['gca_records_key']}/{run_date}"
    local_path = os.path.join(os.getcwd(), s3_key)
    table_name = create_principals_gca_table(redshift_creds, run_date, social_data_append_table)
    populate_table(redshift_creds, table_name, run_date, social_data_append_table)
    unload_from_redshift(redshift_creds, s3_creds, table_name, run_date, social_data_append_table)
    if not os.path.exists(local_path):
        os.mkdir(local_path)
    pf.download_from_s3(s3_creds['bucket'], s3_key)
    transfer_file(s3_creds, sftp_creds, local_path)
    shutil.rmtree(local_path)


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))