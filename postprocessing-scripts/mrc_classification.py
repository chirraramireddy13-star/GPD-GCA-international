import MySQLdb
import sys
import math
import logging

BATCH_STANDARD_QUERY_NAME = "TitleNormalization_FromTable"
V_CLIENT = 148634
V_EXTERNAL_VENDOR_ID = 77
V_PRIORITY = 5
TITLE_RECHECK_TABLE = "scratchpad.ags_titlemrc_recheck_subpop_delete"
TITLE_UNIQUE_TABLE = "matchDist.TitleUnique"

logger = logging.getLogger(__name__)


def filter_titles(db_creds, start_row):
    with MySQLdb.connect(**db_creds) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TITLE_RECHECK_TABLE}")
            cur.execute(f"""CREATE TABLE {TITLE_RECHECK_TABLE} (
                jobtitle VARCHAR(500) UNIQUE,
                titleUnique_id BIGINT,
                rowNum INT PRIMARY KEY auto_increment
            );""")
            cur.execute(f"CREATE INDEX one ON {TITLE_RECHECK_TABLE}(titleUnique_id);")

            query = f"""INSERT INTO {TITLE_RECHECK_TABLE} (jobtitle, titleUnique_id)
                SELECT title,
                       titleUnique_id
                FROM {TITLE_UNIQUE_TABLE} tu
                WHERE tu.titleUnique_id > {start_row}
                AND COALESCE(mrcCode, '') = '';"""
            logger.info(query)
            cur.execute(query)

            query = f"""UPDATE {TITLE_RECHECK_TABLE} r
                SET titleUnique_id = -1
                WHERE r.jobtitle <> CONVERT(r.jobtitle USING ASCII);"""
            logger.info(query)
            cur.execute(query)

            cur.execute(f"""UPDATE {TITLE_RECHECK_TABLE} r
                JOIN {TITLE_UNIQUE_TABLE} tu ON r.titleUnique_id = tu.titleUnique_id
                SET r.titleUnique_id = -1
                WHERE tu.mrcCode <> '';""")
            cur.execute(f"DELETE FROM {TITLE_RECHECK_TABLE} WHERE titleUnique_id = -1;")


def get_job_guid(cur, batch_id):
    cur.execute(f"SELECT job_guid FROM verServices.Batch WHERE batch_id = {batch_id} LIMIT 1")
    return cur.fetchone()[0]


def get_batch_id(cur):
    cur.execute("SELECT @batchz")
    return cur.fetchone()[0]

def population_create(db_creds):
    with MySQLdb.connect(**db_creds) as conn:
        with conn.cursor() as cur:
            cur.callproc("orapa.PopulationCreation", (
                BATCH_STANDARD_QUERY_NAME,
                TITLE_RECHECK_TABLE,
                V_CLIENT,
                V_EXTERNAL_VENDOR_ID,
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                V_PRIORITY,
                "NULL",
                "NULL",
            ))
            batch_id = get_batch_id(cur)
            logger.info(f"{batch_id=}")
            job_guid = get_job_guid(cur, batch_id)
            logger.info(f"{job_guid=}")
            return job_guid, batch_id


def create_batch_csv(db_creds, batch_id):
    logger.info(f"{batch_id=}")
    with MySQLdb.connect(**db_creds) as conn:
        with conn.cursor() as cur:
            cur.callproc("orapa.createBatchCSV", (batch_id,))


def main(argv):
    db_creds = argv[0]
    max_row_before_insert = argv[1]
    db_creds["autocommit"] = True
    filter_titles(db_creds, max_row_before_insert)
    job_guid, batch_id = population_create(db_creds)
    create_batch_csv(db_creds, batch_id)
    return job_guid, batch_id


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))