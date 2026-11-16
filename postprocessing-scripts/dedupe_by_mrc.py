import collections
import csv
import glob
import logging
import mmap
import multiprocessing
import os
import re
import sys
import traceback
from configparser import ConfigParser

import processing_functions as pf

# NOTE: subset of primary MRCs to be considered in title-by-MRC dedupe
DEDUPE_MRC_SET = {
    'A1BA', 'A1A4', 'A1CX', 'A1A7', 'A1A9', 'C2A1', 'A154', 'A149', 'A117', 'A1A8', 'F134', 'E134',
    'I149', 'C2A2', 'XDX1', 'B6H8', 'A0A3', 'A0A1', 'A0A2', 'A1A6', 'A1A5', 'A1CB', 'A1CW', 'A9DW',
}
THOMSON_REUTERS_OWNER_ID = 1
# NOTE: for now i think we only have the one GSRL7 owner ID
GSRL7_PRINCIPALS_OWNER_IDS = {2}

log = logging.getLogger(__name__)


class MRCDedupeCandidate(object):
    def __init__(self, job_title, primary_mrc, phone_accuracy_score, email_deliverability,
                 smart_sort_score, social_verified_flag, data_source_id, duns_id,
                 global_ultimate_duns_id, record_offset, record_end):
        self.job_title = job_title_normalize(job_title)
        self.primary_mrc = primary_mrc
        self.title_groups = []
        self.phone_accuracy_score = int(phone_accuracy_score)
        self.email_deliverability = int(email_deliverability)
        self.smart_sort_score = int(smart_sort_score)
        self.social_verified_flag = int(social_verified_flag)
        self.data_source_id = int(data_source_id)
        self.duns_id = duns_id
        self.global_ultimate_duns_id = global_ultimate_duns_id
        self.record_offset = record_offset
        self.record_end = record_end
        self.winner = False


class TitleGroups(object):
    DEFAULT = 0
    A0A1 = 1
    A0A2 = 2
    A0A3 = 3
    A117 = 4
    A149 = 5
    A154 = 6
    A1A4 = 7
    A1A5 = 8
    A1A6 = 9
    A1A7 = 10
    A1A8 = 11
    A1A9 = 12
    A1BA = 13
    A1CB = 14
    A1CW = 15
    A1CX = 16
    A9DW = 17
    B6H8 = 18
    C2A1 = 19
    C2A2 = 20
    E134 = 21
    F134 = 22
    I149 = 23
    XDX1 = 24
    OWNER = 25
    FOUNDER = 26

    @classmethod
    def min(cls):
        return min(v for _, v in cls.__dict__.items() if isinstance(v, int))

    @classmethod
    def max(cls):
        return max(v for _, v in cls.__dict__.items() if isinstance(v, int))
    
def job_title_normalize(job_title):
    job_title = job_title.lower()
    remove_list = ['.', ' of ', ' and ', ',', '/', '|', '-', 'acting', 'nominating']
    for item in remove_list:
        job_title = job_title.replace(item, '')
    job_title = job_title.replace('cheif', 'chief')
    return job_title


def bad_title_components(title):
    if 'chief' in title:
        return False
    bad_components = ['assist', 'asst', 'assoc', 'deputy', 'vice']
    for bad_component in bad_components:
        if bad_component in title and 'vicechairman' not in title and 'vicechairwoman' not in title and 'service' not in title:
            return True
    return False


PUBLIC_DUNS = None


def get_public_duns(redshift_creds, reuters_date):
    with pf.connect_to_redshift(redshift_creds) as conn:
        with conn.cursor() as cur:
            select_from_tr = f"""
                SELECT DISTINCT WB.global_ultimate_duns_id
                FROM int_staging.thomson_reuters_{reuters_date} TR
                JOIN int_gen.worldbase WB
                    ON TR.duns_id = WB.duns_id
            """
            cur.execute(select_from_tr)
            global PUBLIC_DUNS
            PUBLIC_DUNS = set(item[0] for item in cur.fetchall())
            log.info(f"Found {len(PUBLIC_DUNS):,} public DUNS.")


def assign_title_group_ids(records):
    for record in records:
        for code in ('A0A1', 'A0A2', 'A0A3', 'A117', 'A149', 'A154', 'A1A4', 'A1A5',
                     'A1A6', 'A1A7', 'A1A8', 'A1A9', 'A1BA', 'A1CB', 'A1CW', 'A1CX',
                     'A9DW', 'B6H8', 'C2A1', 'C2A2', 'E134', 'F134', 'I149', 'XDX1'):
            if record.primary_mrc == code:
                record.title_groups.append(getattr(TitleGroups, code))
        if not record.title_groups and 'owner' in record.job_title:
            record.title_groups.append(TitleGroups.OWNER)
        if not record.title_groups and 'founder' in record.job_title:
            record.title_groups.append(TitleGroups.FOUNDER)
        if len(record.title_groups) == 0:
            record.title_groups.append(TitleGroups.DEFAULT)


def _count_duplicates(fpath, fieldname):
    gu_duns_offsets = collections.defaultdict(list)
    log.info(f"Counting GU_DUNS for {fpath}.")
    with open(fpath, 'rb') as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            mm.madvise(mmap.MADV_SEQUENTIAL, 0, len(mm))
            reader = csv.reader(line.decode('utf-8') for line in iter(mm.readline, b''))
            header = next(reader)
            gu_duns_index = header.index(fieldname)
            try:
                # NOTE: set offset with mm.tell() before reading data
                offset = mm.tell()
                for row in reader:
                    gu_duns = int(row[gu_duns_index])
                    record_offset = offset
                    # NOTE: update offset before looping to read more data
                    offset = mm.tell()
                    gu_duns_offsets[gu_duns].append((record_offset, offset))
            except StopIteration:
                log.info('Encountered StopIteration. Continuing.')
                pass
    return gu_duns_offsets


def _resolve_group_tie(records):
    ordered = sorted(records, key=lambda r: (r.global_ultimate_duns_id == r.duns_id,
                                             r.phone_accuracy_score, r.email_deliverability,
                                             r.smart_sort_score, r.social_verified_flag,
                                             r.record_offset), reverse=True)
    return ordered[0] 


def determine_thomson_reuters_winner(records):
    # TODO: if multiple TR-sourced records apply standard resolve ties logic
    for record in records:
        if record.data_source_id == THOMSON_REUTERS_OWNER_ID and TitleGroups.OWNER not in record.title_groups and TitleGroups.FOUNDER not in record.title_groups:
            record.winner = True
            return record


def determine_gsrl7_winner(records, owner_ids, group_id):
    # TODO: if multiple GSRL7-sourced records apply standard resolve ties logic
    for record in records:
        if record.data_source_id in owner_ids and group_id in (TitleGroups.OWNER, TitleGroups.FOUNDER):
            if record.global_ultimate_duns_id == record.duns_id:
                record.winner = True
                return record
            elif record.data_source_id in owner_ids:
                winner = _resolve_group_tie([r for r in records if r.data_source_id in owner_ids])
                winner.winner = True
                return winner


def pick_title_group_winner(records, group_id):
    winner = None
    # Check TR for winners first, followed by GSRL7 and then other sources
    winner = determine_thomson_reuters_winner(records)
    if not winner:
        winner = determine_gsrl7_winner(records, GSRL7_PRINCIPALS_OWNER_IDS, group_id)
    if not winner:
        winner = _resolve_group_tie(records)
        winner.winner = True


def _dedupe_file_by_mrc(fpath, output_path):
    try:
        gu_duns_to_offsets_map = _count_duplicates(fpath, 'global_ultimate_duns_id')
        log.info(f"Found {len(gu_duns_to_offsets_map):,} Global Ultimate DUNS.")
        with open(fpath, 'rb') as f:
            with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
                mm.madvise(mmap.MADV_RANDOM, 0, len(mm))
                reader = csv.reader(line.decode('utf-8') for line in iter(mm.readline, b''))
                header = next(reader)
                header_offset = mm.tell()
                pm_index = header.index('primary_mrc_code')
                jt_index = header.index('job_title')
                pa_index = header.index('phone_accuracy_score')
                ed_index = header.index('email_deliverability')
                ss_index = header.index('smart_sort_score')
                sv_index = header.index('social_verified_flag')
                ds_index = header.index('data_source_id')
                du_index = header.index('duns_id')
                gu_index = header.index('global_ultimate_duns_id')

                # TODO: move output dir name to configs
                parent_folder = os.path.basename(os.path.dirname(fpath))
                filename = os.path.basename(fpath)
                outfile_path = os.path.join(output_path, f'{parent_folder}_{filename}')
                with open(outfile_path, 'wb') as outfile:
                    outfile.write(mm[:header_offset])
                    processed = 0
                    for gu_duns in gu_duns_to_offsets_map:
                        group = gu_duns_to_offsets_map[gu_duns]
                        records = []
                        for (offset, end) in group:
                            mm.seek(offset)
                            try:
                                row = next(reader)
                                r = MRCDedupeCandidate(
                                    job_title=row[jt_index],
                                    primary_mrc=row[pm_index],
                                    phone_accuracy_score=int(row[pa_index] or 0),
                                    email_deliverability=int(row[ed_index] or 0),
                                    smart_sort_score=int(row[ss_index] or 0),
                                    social_verified_flag=int(row[sv_index] or 0),
                                    data_source_id=int(row[ds_index] or 0),
                                    duns_id=row[du_index],
                                    global_ultimate_duns_id=row[gu_index],
                                    record_offset=offset,
                                    record_end=end,
                                )
                                if r.primary_mrc in DEDUPE_MRC_SET:
                                    records.append(r)
                                else:
                                    # NOTE: if PrimaryMRCCode not in MRC_LIST, write without suppression
                                    outfile.write(mm[r.record_offset:r.record_end])
                                    processed += 1
                            except StopIteration:
                                log.info('Encountered StopIteration exception. Continuing.')
                                continue
                        if records:
                            assign_title_group_ids(records)
                            for group_id in range(TitleGroups.min(), TitleGroups.max() + 1):
                                group_records = [r for r in records if not r.winner and group_id in r.title_groups]
                                if group_records:
                                    pick_title_group_winner(group_records, group_id)
                            if gu_duns in PUBLIC_DUNS:
                                for record in records:
                                    if TitleGroups.OWNER in record.title_groups:
                                        record.winner = False
                            for record in records:
                                if record.winner or not record.title_groups:
                                    outfile.write(mm[record.record_offset:record.record_end])
                                    processed += 1
                    log.info(f"Wrote {processed:,} (DUNS, MRC)-deduplicated records from {fpath}.")
    except:
        message = traceback.format_exc()
        raise Exception('\nOriginal Exception:\n\n' + message)

def main(data_path, reuters_date=None, file_globs='*_deduplicated.csv', output_path=''):
    config = ConfigParser()
    config.read('config.ini')
    if not reuters_date:
        date_match = re.search(r'\d{8}', data_path)
        reuters_date = date_match.group()
    redshift_creds = config['redshift_cluster']
    log.info(f"Getting list of public DUNS from {redshift_creds['host']}.")
    get_public_duns(redshift_creds, reuters_date)
    if os.path.exists(data_path):
        if os.path.isfile(data_path):
            _dedupe_file_by_mrc(data_path, output_path)
        if os.path.isdir(data_path):
            input_files = glob.glob(os.path.join(data_path, file_globs))
            log.info(f'Deduping the following files {input_files}')
            promises = []
            # NOTE: limited by memory consumption to a single process
            with multiprocessing.Pool(None, maxtasksperchild=1) as pool:
                for fpath in input_files:
                    p = pool.apply_async(_dedupe_file_by_mrc, args=(fpath, output_path))
                    promises.append(p)
                for promise in promises:
                    promise.get()
    else:
        log.exception(f'Incorrectly specified file or directory: {data_path}.')


if __name__ == '__main__':
    data_path = sys.argv[1]
    reuters_date = sys.argv[2]
    file_globs = sys.argv[3]
    output_path = sys.argv[4]
    main(data_path, reuters_date, file_globs, output_path)