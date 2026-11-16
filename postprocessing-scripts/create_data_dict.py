import csv
import os
import sys
import redshift_connector


def connect_to_redshift():
    conn = redshift_connector.connect(
            host=os.environ['REDSHIFT_HOST'],
            database=os.environ.get('REDSHIFT_DATABASE', 'gsrl7'),
            port=int(os.environ.get('REDSHIFT_PORT', '5439')),
            user=os.environ['REDSHIFT_USER'],
            password=os.environ['REDSHIFT_PASSWORD'],
            )
    conn.autocommit = True
    return conn


def get_fill_rates(run_date):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            query = """
            select 100*count(global_contact_key) / count(*) as GlobalContactKey
            ,100*count(full_name) / count(*) as FullName
            ,100*count(first_name) / count(*) as FirstName
            ,100*count(middle_name) / count(*) as MiddleName
            ,100*count(last_name) / count(*) as LastName
            ,100*count(name_prefix) / count(*) as NamePrefix
            ,100*count(name_suffix) / count(*) as nameSuffix
            ,100*count(job_title) / count(*) as JobTitle
            ,100*count(country_code) / count(*) as CountryCode
            ,100*count(duns_id) / count(*) as DUNS_id
            ,100*count(duns) / count(*) as DUNS
            ,100*count(primary_mrc_code) / count(*) as PrimaryMRCCode
            ,100*count(mrc_codes) / count(*) as MRCCodes
            ,100*count(ceo_indicator) / count(*) as MostSeniorContactIndicator
            ,100*count(email) / count(*) as Email
            ,100*count(email_deliverability) / count(*) as GCA_emailDeliverability
            ,100*count(primary_phone) / count(*) as PrimaryPhone
            ,100*count(primary_phone_region_format) / count(*) as PrimaryPhoneRegionFormat
            ,100*count(primary_phone_extension) / count(*) as PrimaryPhoneExtension
            ,100*count(secondary_phone) / count(*) as SecondaryPhone
            ,100*count(secondary_phone_region_format) / count(*) as SecondaryPhoneRegionFormat
            ,100*count(secondary_phone_extension) / count(*) as SecondaryPhoneExtension
            ,100*count(primary_phone_type) / count(*) as PrimaryPhoneType
            ,100*count(secondary_phone_type) / count(*) as SecondaryPhoneType
            ,100*count(phone_accuracy_score) / count(*) as GCA_PhoneAccuracyScore
            ,100*count(social_verified_flag) / count(*) as GCA_SocialVerifiedFlag
            ,100*count(twitter_profile_url) / count(*) as TwitterProfileURL
            ,100*count(facebook_profile_url) / count(*) as FacebookProfileURL
            ,100*count(linkedin_profile_url) / count(*) as LinkedinProfileURL
            ,100*count(smart_sort_score) / count(*) as GCA_SmartSortScore
            ,100*count(contact_suitability_flags) / count(*) as ContactSuitabilityFlags
            ,100*count(contact_suitability_flags_string) / count(*) as ContactSuitabilityFlagsString
            ,100*count(company_name) / count(*) as BusinessName
            from int_delivered.international_contacts_""" + run_date

            logger.info(query)
            cur.execute(query)
            desc = cur.description
            column_names = [col[0] for col in desc]
            data = [dict(zip(column_names, row)) for row in cur.fetchall()]
            return data[0]


def main(argv):
    run_date = argv[1]
    fill_rates = get_fill_rates(run_date)
    out_file_name = run_date + '/output/gbh_global_' + run_date + '.CONTROL'
    with open('IntoConDataDictionary_template.csv', 'rt') as in_file:
        csv_reader = csv.reader(in_file)
        header = next(csv_reader)
        with open(out_file_name, 'w', newline='') as out_file:
            csv_writer = csv.writer(out_file)
            csv_writer.writerow(header)
            for line in csv_reader:
                line[5] = fill_rates.get(line[0].lower())
                csv_writer.writerow(line)
    return out_file_name

if __name__ == '__main__':
   raise SystemExit(main(sys.argv))