import GPDQualityEnforcer.GPDQualityEnforcer as qenf
import csv
import os
import MySQLdb
import multiprocessing
import glob
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")
sql_creds = config["mysql_creds"]


def suppress_profanity_data(infile_path, output_path):
    file_basename = os.path.basename(infile_path)
    with open(infile_path, 'rt') as infile, \
            open(os.path.join(output_path, "files_for_review", file_basename), 'wt', newline='') as review_file, \
            open(os.path.join(output_path, "files_for_delivery", file_basename), 'wt', newline='') as delivery_file:

        reader = csv.reader(infile)
        header = next(reader)
        first_name_index = header.index("first_name")
        middle_name_index = header.index("middle_name")
        last_name_index = header.index("last_name")
        prefix_index = header.index("name_prefix")
        suffix_index = header.index("name_suffix")
        title_index = header.index("job_title")

        review_writer = csv.writer(review_file)
        review_writer.writerow(header)

        delivery_writer = csv.writer(delivery_file)
        delivery_writer.writerow(header)

        with MySQLdb.connect(host=sql_creds["host"], user=sql_creds["user"], password=sql_creds["password"], db="dopeRef") as conn, \
                MySQLdb.connect(host=sql_creds["host"], user=sql_creds["user"], password=sql_creds["password"], db="banWordRef") as bwconn:
            qenf_obj = qenf.GPDQualityEnforcer(conn, bwconn)

            for row in reader:
                contains_unwanted_words = qenf_obj.detect_unwanted_words(
                    title=row[title_index],
                    firstName=row[first_name_index],
                    middleName=row[middle_name_index],
                    lastName=row[last_name_index],
                    prefix=row[prefix_index],
                    suffix=row[suffix_index]
                )
                if contains_unwanted_words[0]:
                    # print(contains_unwanted_words[2], contains_unwanted_words[1])
                    review_writer.writerow(row)
                else:
                    delivery_writer.writerow(row)

def main(output_path, files):
    # no_of_cores_to_use = os.cpu_count() // 2
    mp_pool = multiprocessing.Pool(None, maxtasksperchild=1)
    promises = []

    try:
        for file in files:
            promises.append((file, mp_pool.apply_async(suppress_profanity_data, args=(file, output_path))))
        mp_pool.close()

        for f, promise in promises:
            promise.get()

    except Exception as e:
        print(f"An Exception occurred: {e}")

    finally:
        mp_pool.join()


def run_profanity_check(output_path):
    delivery_file_path = os.path.join(output_path, "files_for_delivery")
    review_file_path = os.path.join(output_path, "files_for_review")
    os.makedirs(delivery_file_path, exist_ok=True)
    os.makedirs(review_file_path, exist_ok=True)
    files = glob.glob(os.path.join(output_path, "*.csv"), recursive=True)
    main(output_path, files)


if __name__ == "__main__":
    path = os.getcwd()
    # path = r'/local/international-contacts/YYYYMMDD'
    output_path = os.path.join(path, "output")
    run_profanity_check(output_path)