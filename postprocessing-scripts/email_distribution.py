import redshift_connector
from openpyxl import Workbook
from configparser import ConfigParser
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_redshift(creds):
    try:
        conn = redshift_connector.connect(
            host=creds["host"],
            database=creds["database"],
            port=creds["port"],
            user=creds["user"],
            password=creds["password"]
        )
        conn.autocommit = True
        logging.info("Connected to Redshift")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Redshift: {e}")  # Log the actual exception
        sys.exit(1)


def fetch_data(cursor, query):
    cursor.execute(query)
    return cursor.fetchall(), [desc[0] for desc in cursor.description]


def main(argv):
    if len(argv) < 2:
        logging.error("Usage: python newemails.py <run_date>")
        sys.exit(1)

    run_date = argv[1]
    config = ConfigParser()
    config.read("config.ini")

    redshift_creds = config["redshift_cluster"]

    try:
        with connect_to_redshift(redshift_creds) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CALL sg.generate_email_deliverability_distribution('{run_date}')")

                table1_result, table1_columns = fetch_data(cur, "SELECT * FROM sg.EV_Score_Delivered order by emailverification_score DESC")
                table2_result, table2_columns = fetch_data(cur, "SELECT * FROM sg.EV_Score_Staging order by ev_score DESC")
                table3_result, table3_columns = fetch_data(cur, "select ev_score , count(*) from sg.ev_score_less_than_60 where ev_score <= 60 group by ev_score order by ev_score DESC")

                wb = Workbook()

                ws1 = wb.active
                ws1.title = "Delivered"
                ws1.append(table1_columns)
                for row in table1_result:
                    ws1.append(row)

                ws2 = wb.create_sheet(title="Staging")
                ws2.append(table2_columns)
                for row in table2_result:
                    ws2.append(row)

                ws3 = wb.create_sheet(title="EV Score less than 60")
                ws3.append(table3_columns)
                for row in table3_result:
                    ws3.append(row)

                wb.save("Email_Distribution.xlsx")
                logging.info("Data written to Output.xlsx")

        with connect_to_redshift(redshift_creds) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS sg.EV_Score_Delivered")
                cur.execute("DROP TABLE IF EXISTS sg.EV_Score_Staging")
                cur.execute("DROP TABLE IF EXISTS sg.ev_score_less_than_60")
                cur.execute("DROP TABLE IF EXISTS sg.ev_scorefully")
                logging.info("Temporary tables dropped successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)