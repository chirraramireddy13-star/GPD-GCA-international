import collections
import csv
import functools
import glob
import logging
import mmap
import multiprocessing
import os
import os.path
import sys
import traceback

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')
log = logging.getLogger(__name__)


class IntConDedupeCandidate(object):
    __slots__ = ['first_name', 'middle_name', 'last_name', 'job_title', 'duns', 'global_contact_key', 'data_source', 'record_file', 'record_offset', 'record_end']

    def __init__(self, first_name, middle_name, last_name, job_title, duns, global_contact_key, data_source, record_file=None, record_offset=None, record_end=None):
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.job_title = job_title
        self.duns = duns
        self.global_contact_key = global_contact_key
        self.data_source = data_source
        self.record_file = record_file
        self.record_offset = record_offset
        self.record_end = record_end


def _count_duplicates(parent_dir, data_dir):
    part_files = glob.glob(os.path.join(parent_dir, data_dir, '*.out.csv'))
    gck_offsets = collections.defaultdict(list)
    for fname in part_files:
        log.info(f'Counting GCKs for {fname}.')
        with open(fname, 'rb') as f:
            with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
                mm.madvise(mmap.MADV_SEQUENTIAL, 0, len(mm))
                reader = csv.reader(line.decode('utf-8') for line in iter(mm.readline, b''))
                header = next(reader)
                gck_index = header.index('global_contact_key')
                try:
                    # NOTE: need to set offset with mm.tell() before reading data as reading will move our position
                    offset = mm.tell()
                    for row in reader:
                        global_contact_key = int(row[gck_index])
                        record_file = sys.intern(fname)
                        record_offset = offset
                        # NOTE: update offset before looping which would read more data and move our position
                        offset = mm.tell()
                        gck_offsets[global_contact_key].append((record_file, record_offset, offset))
                except StopIteration:
                    log.info('Encountered StopIteration. Continuing.')
                    continue
    return gck_offsets


def _select_survivor(record, other):
    if not record:
        return other
    if not other:
        return record
    if id(record) == id(other):
        return record
    # TODO: i believe we intend to add more to this
    return min(record, other, key=lambda r: int(r.data_source))

def _deduplicate_directory(parent_dir, subdir):
    try:
        gck_counts = _count_duplicates(parent_dir, subdir)
        log.info('Found {:,} Global Contact Keys.'.format(len(gck_counts)))

        file_map = dict()
        part_files = glob.glob(os.path.join(parent_dir, subdir, '*.out.csv'))
        header = None
        for fname in part_files:
            f = open(fname, 'rb')
            mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
            mm.madvise(mmap.MADV_RANDOM, 0, len(mm))
            reader = csv.reader(line.decode('utf-8') for line in iter(mm.readline, b''))
            if header is None:
                header = next(reader)
                header_offset = mm.tell()
            file_map[sys.intern(fname)] = (mm, reader)

        fn_index = header.index('first_name')
        mn_index = header.index('middle_name')
        ln_index = header.index('last_name')
        jt_index = header.index('job_title')
        dn_index = header.index('duns')
        gck_index = header.index('global_contact_key')
        ds_index = header.index('data_source_id')

        with open(os.path.join(parent_dir, subdir, subdir + '_deduplicated.csv'), 'wb') as outfile:
            # NOTE: files have identical headers, i don't care that offset is from the first and mm here is the last
            outfile.write(mm[:header_offset])
            processed = 0
            c = collections.Counter()
            for gck in gck_counts:
                group = gck_counts[gck]
                records = []
                for (fname, offset, end) in group:
                    c[fname] += 1
                    mm, reader = file_map[fname]
                    mm.seek(offset)
                    try:
                        row = next(reader)
                        r = IntConDedupeCandidate(
                            first_name=row[fn_index],
                            middle_name=row[mn_index],
                            last_name=row[ln_index],
                            job_title=row[jt_index],
                            duns=row[dn_index],
                            global_contact_key=row[gck_index],
                            data_source=row[ds_index],
                            record_file=fname,
                            record_offset=offset,
                            record_end=end,
                        )
                        records.append(r)
                    except StopIteration:
                        log.info('Encountered StopIteration exception. Continuing.')
                        continue

                if records:
                    winner = functools.reduce(_select_survivor, records)
                    mm, reader = file_map[winner.record_file]
                    outfile.write(mm[winner.record_offset:winner.record_end])
                    processed += 1
            log.info(f'Processed a total of {processed}')
            log.info(f'Filename counts: {c.most_common(5)}')
    except:
        message = traceback.format_exc()
        raise Exception('\nOriginal Exception:\n\n' + message)


def main(data_dir, sub_dir=None):
    if sub_dir:
        _deduplicate_directory(data_dir, sub_dir)
    else:
        # NOTE: unfortunately high memory consumption currently has us running dedupe in series
        pool = multiprocessing.Pool(processes=1, maxtasksperchild=1)
        promises = []
        subdirs = os.listdir(data_dir)
        for d in subdirs:
            if os.path.isdir(os.path.join(data_dir, d)) and d.startswith('mod_'):
                promise = pool.apply_async(_deduplicate_directory, (data_dir, d))
                promises.append(promise)
        pool.close()
        pool.join()
        for promise in promises:
            # TODO: needs try except, logging
            promise.get()
        pool.terminate()


if __name__ == '__main__':
    data_dir = sys.argv[1]
    sub_dir = None
    if len(sys.argv) > 2:
        sub_dir = sys.argv[2]
    main(data_dir, sub_dir)