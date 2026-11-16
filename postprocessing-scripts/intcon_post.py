from __future__ import print_function, unicode_literals
import logging
import time
import multiprocessing
import os.path
import sys
import csv
import traceback
import functools
from banwords import banwords, generated_banword
from GPDNormalizer import GPDNormalizer
import emoji
from suppressions import BYTE_SUPPRESSIONS
from configparser import ConfigParser
import ahocorasick
import suppress
from mrc_scores import mrc_code_to_smart_sort_score_dict
import processing_functions as pf

logger = logging.getLogger(__name__)

SPECIAL_CHARACTERS = "!@#$%^&*()-+?_=,.<>/;: '"
DIGITS = "0123456789"
BANWORD_TABLE = "int_gen.banword"

config = ConfigParser()
config.read("config.ini")
REDSHIFT_CREDS = config["redshift_cluster"]


def build_automatons(banword_dict):
    wildcard = ahocorasick.Automaton()
    wildcard_list = list()
    for record in banword_dict:
        if banword_dict.get(record).get("match_type") == "wildcard":
            data = {
                "value": record,
                "except_field": banword_dict.get(record).get("except_field"),
            }
            idx = len(wildcard_list)
            wildcard_list.append(data)
            wildcard.add_word(str(record), idx)
    wildcard.make_automaton()
    return wildcard, wildcard_list


def get_banwords():
    with pf.connect_to_redshift(REDSHIFT_CREDS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT LOWER(TRIM(value)) AS value,
                       matchtype AS match_type,
                       exceptfield AS except_field
                       FROM {BANWORD_TABLE}
                       WHERE COALESCE(match_type, '') <> ''
                       AND COALESCE(except_field, '') <> ''
                       AND category <> 'Certification'""")
            banwords = cur.fetchall()
            banwords_dict = dict()
            for word in banwords:
                banwords_dict[word[0]] = {"match_type": word[1], "except_field": word[2]}
    return banwords_dict


def has_banword(value, type_value, banword_dict, wildcard_automaton, wildcard_list):
    check_word = value.strip().lower()
    word_data = banword_dict.get(check_word)
    if word_data and word_data.get("match_type") == "exactWord":
        if word_data.get("except_field") is None or type_value not in word_data.get("except_field", {}):
            return True
    for end_idx, rule_idx in wildcard_automaton.iter(check_word):
        rule = wildcard_list[rule_idx]
        except_field = rule.get("except_field")
        if type_value in except_field:
            continue
        return True
    return False

def set_smart_sort_score(**kwargs):
    primary_mrc_code = kwargs["primary_mrc_code"]
    smart_sort_score = mrc_code_to_smart_sort_score_dict.get(primary_mrc_code, 60)
    phone_accuracy_score = int(kwargs["phone_accuracy_score"]) if kwargs["phone_accuracy_score"] else 0
    ev_score = int(kwargs["ev_score"]) if kwargs["ev_score"] else 0
    new_linkedin_value = 100 if kwargs["linkedin_profile_url"] else 0
    global_ultimate_duns_id = kwargs["global_ultimate_duns_id"]
    duns_id = kwargs["duns_id"]
    ceo_indicator = int(kwargs["ceo_indicator"])
    data_source_id = int(kwargs["data_source_id"])
    data_sources_dict = kwargs["data_sources_dict"]
    smart_sort_title_score = smart_sort_score
    smart_sort_formula = int(0.2 * phone_accuracy_score) + int(0.5 * ev_score) + int(0.3 * new_linkedin_value)
    smart_sort_score += smart_sort_formula
    if smart_sort_score < 300 or smart_sort_formula < 75:
        if smart_sort_formula * 3 > smart_sort_title_score:
            smart_sort_score = smart_sort_formula * 3
    bump = 0
    data_source_name = data_sources_dict.get(int(data_source_id))
    if ceo_indicator:
        if global_ultimate_duns_id == duns_id:
            bump = 300
        else:
            bump = 200
    if data_source_name == 'GSRL7':
        bump += 1000
    elif data_source_name == 'ThomsonReuters':
        bump += 3000
    elif data_source_name == 'Orb':
        bump += 2000
    smart_sort_score += bump
    return smart_sort_score


def maintain_exceptions(fn):
    @functools.wraps(fn)
    def func(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except:
            raise Exception("original traceback:\n" + traceback.format_exc())
    return func

def _window(iter_, size=3):
    start = 0
    while True:
        y = iter_[start:start+size]
        if len(y) < size:
            break
        yield y
        start += 1


def is_english(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


def normalize_phone(phone, country_code):
    result = None
    regional_phone = None
    extension = None
    if phone and country_code:
        try:
            ok, data, _, _ = GPDNormalizer.GPDNormalizer.Normalize_phone_google(str(phone), country_code)
            if ok:
                regional_phone = data["Regional"]
                result = data["E164"]
                extension = data["Extension"]
        except Exception as e:
            logger.error(e)
            pass
    return result, regional_phone, extension


def is_valid_url(url):
    result = True
    if url:
        result, _, _ = GPDNormalizer.Normalize_website(url)
    return result


def has_three_consecutive_special_chars(value):
    count = 0
    for char in value:
        if char in SPECIAL_CHARACTERS or char.encode("utf-8") in BYTE_SUPPRESSIONS:
            count += 1
            if count >= 3:
                return True
        else:
            count = 0


def contains_emoji(value):
    return any(emoji.is_emoji(char) for char in value)

def has_digit(value):
    if value in DIGITS:
        return True


def is_valid_title(title):
    if title:
        if len(title) < 3: return False
        lower_title = title.lower()
        if lower_title.isalpha():
            return True
        elif "mr " in lower_title: return False
        elif "mrs " in lower_title: return False
        elif has_three_consecutive_special_chars(lower_title):
            return False
        digit_count = 0
        for char in lower_title:
            if has_digit(char):
                digit_count += 1
                if digit_count == 3:
                    return False
        for w in _window(lower_title):
            if len(set(w)) == 1:
                return False
        if contains_emoji(lower_title):
            return False
        elif any(char.encode("utf-8") in BYTE_SUPPRESSIONS for char in lower_title):
            return False
        return True
    return False


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
        elif any(char.encode("utf-8") in BYTE_SUPPRESSIONS for char in lower_name):
            return False
        return True
    return False


def get_outfile_path(infile_path, outfile_ext):
    root, ext = os.path.splitext(infile_path)
    return root + outfile_ext

def get_data_sources(run_date):
    data_source_dict = dict()
    with pf.connect_to_redshift(REDSHIFT_CREDS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT data_source_id, source_name FROM int_metrics.sfmp_run_sources_{run_date}")
            data = cur.fetchall()
    for record in data:
        data_source_dict[record[0]] = record[1]
    return data_source_dict


@maintain_exceptions
def process_file(**kwargs):
    skipped_profanity = 0
    skipped_decoding = 0
    infile_path = kwargs.get("input_file")
    outfile_path = kwargs.get("output_path")
    data_sources_dict = kwargs.get("data_sources_dict")
    banword_dict = kwargs.get("banwords_dict")
    wildcard_automaton = kwargs.get("wildcard_automaton")
    wildcard_list = kwargs.get("wildcard_list")

    with open(infile_path, "rt") as infile, open(outfile_path, "wt", newline='') as outfile:
        logger.info("Working on file: %s", infile_path)
        reader = csv.reader(infile)
        header = next(reader)
        full_name_index = header.index("full_name")
        first_name_index = header.index("first_name")
        middle_name_index = header.index("middle_name")
        last_name_index = header.index("last_name")
        primary_phone_index = header.index("primary_phone")
        primary_phone_score = header.index("phone_accuracy_score")
        primary_phone_type = header.index("primary_phone_type")
        phone_extension = header.index("primary_phone_extension")
        regional_phone = header.index("primary_phone_region_format")
        title_index = header.index("job_title")
        country_code_index = header.index("country_code")
        linkedin_index = header.index("linkedin_profile_url")
        senior_contact_indicator = header.index("ceo_indicator")
        gca_social_verified = header.index("social_verified_flag")
        smart_sort_score = header.index("smart_sort_title_score")
        ev_score = header.index("email_deliverability")
        data_source_id = header.index("data_source_id")
        global_ultimate_duns_id = header.index("global_ultimate_duns_id")
        duns_id = header.index("duns_id")
        name_prefix = header.index("name_prefix")
        name_suffix = header.index("name_suffix")
        email = header.index("email")
        primary_mrc_code = header.index("primary_mrc_code")

        header = pf.get_intcon_header()
        writer = csv.writer(outfile, dialect='unix')
        writer.writerow(header)
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except UnicodeDecodeError:
                logger.info("unicode error")
                skipped_decoding += 1
                continue
            except:
                continue

            if not is_valid_title(row[title_index]) or len(row[title_index]) > 200:
                row[title_index] = 'Employee'
            if not is_valid_name(row[full_name_index]) or (
                (data_sources_dict.get(int(row[data_source_id])) != 'GSRL7') and
                (len(row[first_name_index]) > 30 or
                 len(row[last_name_index]) > 30 or
                 len(row[middle_name_index]) > 30 or
                 len(row[name_prefix]) > 30 or
                 len(row[name_suffix]) > 30 or
                 len(row[full_name_index]) > 90)):
                continue
            if row[email] and len(row[email]) > 80:
                row[email] = None
                row[ev_score] = None
            if ('|' in row[first_name_index] or '|' in row[last_name_index] or
                '|' in row[middle_name_index] or '|' in row[full_name_index] or
                row[full_name_index].startswith('=-')):
                continue
            if (has_banword(row[title_index], "jobTitle", banword_dict, wildcard_automaton, wildcard_list) or
                has_banword(row[full_name_index], "fullName", banword_dict, wildcard_automaton, wildcard_list) or
                has_banword(row[first_name_index], "firstName", banword_dict, wildcard_automaton, wildcard_list) or
                has_banword(row[middle_name_index], "middleName", banword_dict, wildcard_automaton, wildcard_list) or
                has_banword(row[last_name_index], "lastName", banword_dict, wildcard_automaton, wildcard_list)):
                skipped_profanity += 1
                continue
            if row[country_code_index] == "BR":
                original_phone = row[primary_phone_index]
                row[primary_phone_index], row[regional_phone], row[phone_extension] = normalize_phone(row[primary_phone_index], row[country_code_index])
                if row[primary_phone_index] is None and original_phone != "":
                    row[primary_phone_index] = original_phone
            else:
                row[primary_phone_index], row[regional_phone], row[phone_extension] = normalize_phone(row[primary_phone_index], row[country_code_index])
                if row[primary_phone_index] is None:
                    row[primary_phone_score] = None
                    row[primary_phone_type] = None
            if ((row[primary_phone_index] and len(row[primary_phone_index]) > 30) or
                (row[regional_phone] and len(row[regional_phone]) > 30)):
                row[primary_phone_index] = None
                row[regional_phone] = None
            if row[linkedin_index] and len(row[linkedin_index]) > 92:
                row[linkedin_index] = None
            if row[senior_contact_indicator].lower() in ('true', 't'):
                row[senior_contact_indicator] = 1
            elif row[senior_contact_indicator].lower() in ('false', 'f'):
                row[senior_contact_indicator] = 0
            if row[gca_social_verified].lower() in ('true', 't'):
                row[gca_social_verified] = 1
            elif row[gca_social_verified].lower() in ('false', 'f'):
                row[gca_social_verified] = 0
            if "|" in row[title_index]:
                row[title_index] = row[title_index].replace('|', '/')
            score_kwargs = {
                "smart_sort_title_score": row[smart_sort_score],
                "ev_score": row[ev_score],
                "data_source_id": row[data_source_id],
                "phone_accuracy_score": row[primary_phone_score],
                "global_ultimate_duns_id": row[global_ultimate_duns_id],
                "ceo_indicator": row[senior_contact_indicator],
                "duns_id": row[duns_id],
                "linkedin_profile_url": row[linkedin_index],
                "data_sources_dict": data_sources_dict,
                "primary_mrc_code": row[primary_mrc_code],
            }
            row[smart_sort_score] = set_smart_sort_score(**score_kwargs)
            if (suppress.special_name_suppression(row, first_name_index, last_name_index, full_name_index)
                    or suppress.identical_name_field_value_suppression(row, first_name_index, last_name_index, full_name_index, title_index)
                    or suppress.suppress_invalid_names_by_country(row, first_name_index, last_name_index, country_code_index)):
                continue

            writer.writerow(row)

    return (skipped_profanity, skipped_decoding)

def main(run_date, files):
    skipped_decoding = 0
    skipped_profanity = 0

    data_sources_dict = get_data_sources(run_date)
    banwords_dict = get_banwords()
    wildcard_automaton, wildcard_list = build_automatons(banwords_dict)

    if len(files) > 1:
        mp_pool = multiprocessing.Pool(None, maxtasksperchild=1)
        promises = list()

        for file_ in files:
            outfile_path = get_outfile_path(file_, ".out.csv")
            args = {"data_sources_dict": data_sources_dict,
                    "input_file": file_,
                    "output_path": outfile_path,
                    "wildcard_automaton": wildcard_automaton,
                    "wildcard_list": wildcard_list,
                    "banwords_dict": banwords_dict}
            promises.append((file_, outfile_path, mp_pool.apply_async(process_file, kwds=args),))

        mp_pool.close()
        while promises:
            p = promises.pop(0)
            try:
                profanity, decoding = p[2].get(0)
                skipped_profanity += profanity
                skipped_decoding += decoding
            except multiprocessing.TimeoutError:
                promises.append(p)
            except:
                mp_pool.terminate()
                raise

        mp_pool.terminate()
        logger.info("skipped profanity: %s", skipped_profanity)
        logger.info("skipped decoding: %s", skipped_decoding)
        return 0
    else:
        outfile_path = get_outfile_path(files[0], ".out.csv")
        args = {"data_sources_dict": data_sources_dict,
                "input_file": files[0],
                "output_path": outfile_path,
                "wildcard_automaton": wildcard_automaton,
                "wildcard_list": wildcard_list,
                "banwords_dict": banwords_dict}

        skipped_profanity, skipped_decoding = process_file(**args)
        logger.info("skipped profanity: %s", skipped_profanity)
        logger.info("skipped decoding: %s", skipped_decoding)
        return 0


if __name__ == "__main__":
    run_date = sys.argv[1]
    files = sys.argv[2:]
    raise SystemExit(main(run_date, files))