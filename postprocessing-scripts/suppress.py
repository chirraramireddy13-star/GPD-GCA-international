SPECIAL_NAMES = {"teacher", "prof.", "prof", "professor", "dr", "dr.", "doctor", "jr.", "jr", "junior", "sr.", "sr", "senior"}
COUNTRY_CODES = ("US", "GB", "IE", "CA")


def special_name_suppression(row, first_name_index, last_name_index, full_name_index):
    first_name = row[first_name_index].strip().lower() or u""
    last_name = row[last_name_index].strip().lower() or u""
    full_name = row[full_name_index].strip().lower() or u""

    if (((first_name == u"" or last_name == u"") and full_name == u"") or
            first_name in SPECIAL_NAMES or
            last_name in SPECIAL_NAMES or
            full_name in SPECIAL_NAMES or
            u"%" in first_name or
            u"%" in last_name or
            u"%" in full_name or
            u"??" in first_name or
            u"??" in last_name or
            u"??" in full_name):
        return True
    return False


def identical_name_field_value_suppression(row, first_name_index, last_name_index, full_name_index, job_title_index):
    first_name = row[first_name_index].strip().lower() or u""
    last_name = row[last_name_index].strip().lower() or u""
    job_title = row[job_title_index].strip().lower() or u""
    full_name = row[full_name_index].strip().lower() or u""

    if job_title and (job_title == full_name or job_title == first_name or job_title == last_name):
        return True

    if first_name == '' and last_name == '':
        return False
    elif first_name == last_name:
        return True
    return False


def suppress_invalid_names_by_country(row, first_name_index, last_name_index, country_code_index):
    last_name = row[last_name_index].strip().lower() or u""
    first_name = row[first_name_index].strip().lower() or u""
    country_code = row[country_code_index].strip().upper() or u""

    if country_code in COUNTRY_CODES and (last_name is None or str(last_name) == "" or
                                          str(last_name) in ("null", "none")):
        return True
    if country_code in COUNTRY_CODES and (first_name is None or str(first_name) == "" or
                                          str(first_name) in ("null", "none")):
        return True
    return False