import os
import sys

import MySQLdb
import redshift_connector


def execute_query(data, conn, cur):
    query = """INSERT IGNORE INTO Intl_Publish_Counts(
        countdate,
        country_code,
        total_records,
        total_emails,
        distinct_emails,
        total_phone,
        total_phone_dd,
        distinct_phone,
        email_and_or_phone_dd,
        email_and_linkedin,
        total_linkedin,
        distinct_linkedin)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cur.executemany(query, data)
    conn.commit()


def insert_into_table(data):
    db_creds = {
        "host": os.environ["MYSQL_HOST"],
        "user": os.environ["MYSQL_USER"],
        "passwd": os.environ["MYSQL_PASSWORD"],
        "db": os.environ["MYSQL_DATABASE"],
        "use_unicode": True,
    }
    with MySQLdb.connect(**db_creds) as conn:
        with conn.cursor() as cur:
            execute_query(data, conn, cur)


def get_data(run_date):
    with connect_to_redshift() as conn:
        with conn.cursor() as cur:
            query = """SELECT DATE('""" + run_date + """') AS countdate,
                countrycode,
                COUNT(*) AS total_records,
                SUM(CASE WHEN intl.email IS NOT NULL THEN 1 ELSE 0 END) AS total_emails,
                COUNT(DISTINCT intl.email) AS distinct_emails,
                SUM(CASE WHEN intl.primaryphone IS NOT NULL THEN 1 ELSE 0 END) AS total_phone,
                SUM(CASE WHEN intl.primaryphone IS NOT NULL AND primaryphonetype LIKE '%direct%' THEN 1 ELSE 0 END) AS total_phone_dd,
                COUNT(DISTINCT intl.primaryphone) AS distinct_phone,
                SUM(CASE WHEN intl.email IS NOT NULL OR (intl.primaryphone IS NOT NULL AND primaryphonetype LIKE '%direct%') THEN 1 ELSE 0 END) AS email_and_or_phone_dd,
                SUM(CASE WHEN intl.email IS NOT NULL AND intl.linkedinprofileurl IS NOT NULL THEN 1 ELSE 0 END) AS email_and_linkedin,
                SUM(CASE WHEN intl.linkedinprofileurl IS NOT NULL THEN 1 ELSE 0 END) AS total_linkedin,
                COUNT(DISTINCT intl.linkedinprofileurl) AS distinct_linkedin
            FROM int_delivered.international_contacts_""" + run_date + """ intl
            GROUP BY countdate, countrycode"""
            cur.execute(query)
            return [row for row in cur.fetchall()]


def connect_to_redshift():
    conn = redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        database=os.environ["REDSHIFT_DATABASE"],
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    conn.autocommit = True
    return conn


def main(argv):
    run_date = argv[1]
    data = get_data(run_date)
    insert_into_table(data)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))