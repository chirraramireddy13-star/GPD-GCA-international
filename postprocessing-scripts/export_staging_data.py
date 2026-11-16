import processing_functions as pf
from configparser import ConfigParser
import logging
import sys

config = ConfigParser()
config.read("config.ini")
sftp_creds = config["km"]
redshift_creds = config["redshift_cluster"]
s3_creds = config["s3_creds"]

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s")
logger = logging.getLogger(__name__)


def main(suffix):
    mod_val = 28
    USA_ISO2 = "US"
    CANADA_ISO2 = "CA"
    EMPTY_STR = "''"

    logger.info(f"Unloading with {mod_val} from int_staging.international_contacts_{suffix}")

    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            cur.execute("SET enable_case_sensitive_identifier TO True")
            for i in range(mod_val):
                query_txt = f"""UNLOAD
                    ('WITH base AS (
                        SELECT
                            i.global_contact_key,
                            i.full_name,
                            i.first_name,
                            i.middle_name,
                            i.last_name,
                            i.name_prefix,
                            i.name_suffix,
                            i.job_title,
                            i.country_code,
                            i.duns_id,
                            i.duns,
                            i.domestic_ultimate_duns_id,
                            i.global_ultimate_duns_id,
                            i.primary_mrc_code,
                            i.mrc_codes,
                            i.primary_mrc_code_description,
                            i.ceo_indicator,
                            CASE WHEN country_code IN (SELECT iso2_country_code FROM int_gen.whitelist_blacklist_markets WHERE email_whitelist IS True) THEN email ELSE NULL END AS email,
                            CASE WHEN country_code IN (SELECT iso2_country_code FROM int_gen.whitelist_blacklist_markets WHERE email_whitelist IS True) THEN email_deliverability ELSE NULL END AS email_deliverability,
                            CASE WHEN (
                                (wb.iso2_country_code = ''{USA_ISO2}'' AND LOWER(wb.state_providence) = ''colorado'')
                                OR
                                (wb.iso2_country_code = ''{CANADA_ISO2}'' AND LOWER(wb.state_providence) = ''quebec'')
                            ) THEN NULL
                            WHEN country_code IN (SELECT iso2_country_code FROM int_gen.whitelist_blacklist_markets WHERE is_whitelist IS True) THEN i.primary_phone
                            ELSE NULL
                            END AS primary_phone,
                            CASE WHEN (
                                (wb.iso2_country_code = ''{USA_ISO2}'' AND LOWER(wb.state_providence) = ''colorado'')
                                OR
                                (wb.iso2_country_code = ''{CANADA_ISO2}'' AND LOWER(wb.state_providence) = ''quebec'')
                            ) THEN NULL
                            ELSE i.secondary_phone
                            END AS secondary_phone,
                            i.primary_phone_region_format,
                            i.primary_phone_extension,
                            i.secondary_phone_region_format,
                            i.secondary_phone_extension,
                            i.primary_phone_type,
                            i.secondary_phone_type,
                            i.phone_accuracy_score,
                            i.social_verified_flag,
                            i.twitter_profile_url,
                            i.facebook_profile_url,
                            i.linkedin_profile_url,
                            i.instagram_profile_url,
                            i.gca_contact_reachability_score,
                            t.smart_sort_title_score,
                            i.contact_suitability_flags,
                            i.contact_suitability_flags_string,
                            i.company_name,
                            i.data_source_id,
                            i.email_source_id,
                            i.phone_source_id,
                            i.source_unique_id,
                            i.suppressed,
                            i.match_key,
                            i.contact_id,
                            i.email_presence_indicator,
                            i.email_domain,
                            i.gca_owner_id,
                            i.gca_title_match_flag,
                            i.gca_validity,
                            i.gca_vanity_title,
                            i.individual_id,
                            i.role_ids,
                            i.row_num,
                            i.gcm_verification_timestamp
                        FROM int_staging.international_contacts_{suffix} i
                        JOIN gsr17.int_gen.worldbase wb
                            ON i.duns_id = wb.duns_id
                        LEFT JOIN int_gen.smart_sort_title_score t
                            ON i.primary_mrc_code = t.mrc_code
                        WHERE
                            MOD(i.global_ultimate_duns_id, {mod_val}) = {i}
                            AND suppressed IS NOT True
                            AND full_name IS NOT NULL
                            AND COALESCE(wb.global_ultimate_duns_id, 0) != 0
                    )
                    SELECT
                        global_contact_key,
                        full_name,
                        first_name,
                        middle_name,
                        last_name,
                        name_prefix,
                        name_suffix,
                        job_title,
                        country_code,
                        duns_id,
                        duns,
                        domestic_ultimate_duns_id,
                        global_ultimate_duns_id,
                        primary_mrc_code,
                        mrc_codes,
                        primary_mrc_code_description,
                        ceo_indicator,
                        email,
                        email_deliverability,
                        primary_phone,
                        CASE WHEN COALESCE(primary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE primary_phone_region_format
                        END AS primary_phone_region_format,
                        CASE WHEN COALESCE(primary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE primary_phone_extension
                        END AS primary_phone_extension,
                        secondary_phone,
                        CASE WHEN COALESCE(secondary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE secondary_phone_region_format
                        END AS secondary_phone_region_format,
                        CASE WHEN COALESCE(secondary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE secondary_phone_extension
                        END AS secondary_phone_extension,
                        CASE WHEN COALESCE(primary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE primary_phone_type
                        END AS primary_phone_type,
                        CASE WHEN COALESCE(secondary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE secondary_phone_type
                        END AS secondary_phone_type,
                        CASE WHEN COALESCE(primary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE phone_accuracy_score
                        END AS phone_accuracy_score,
                        social_verified_flag,
                        twitter_profile_url,
                        facebook_profile_url,
                        linkedin_profile_url,
                        instagram_profile_url,
                        gca_contact_reachability_score,
                        smart_sort_title_score,
                        contact_suitability_flags,
                        contact_suitability_flags_string,
                        company_name,
                        data_source_id,
                        email_source_id,
                        CASE WHEN COALESCE(primary_phone, {EMPTY_STR}) = {EMPTY_STR} THEN NULL
                            ELSE phone_source_id
                        END AS phone_source_id,
                        source_unique_id,
                        suppressed,
                        match_key,
                        contact_id,
                        email_presence_indicator,
                        email_domain,
                        gca_owner_id,
                        gca_title_match_flag,
                        gca_validity,
                        gca_vanity_title,
                        individual_id,
                        role_ids,
                        row_num,
                        gcm_verification_timestamp
                    FROM base
                    ')
                    TO 's3://{s3_creds["bucket"]}/post-processing/{suffix}/mod_{i:02}/'
                    IAM_ROLE '{s3_creds["iam_role"]}'
                    format AS csv
                    gzip
                    HEADER
                    ALLOWOVERWRITE
                    ;"""

                logger.info(query_txt)
                cur.execute(query_txt)

if __name__ == "__main__":
    suffix = sys.argv[1]
    raise SystemExit(main(suffix))

