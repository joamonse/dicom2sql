import logging
import argparse
import configparser
import os
from time import strftime, gmtime, perf_counter_ns

import pydicom
import sqlalchemy
from pydicom.errors import InvalidDicomError

from dicom2sql.shared import parse_args, parse_config
from sql.database import Database
from dicom2sql.filesystem.file_extractor import FileExtractor

import csv

from pathlib import Path


def upload_tags_description(csv_path: str, db: Database):
    with open(csv_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        rows = []
        for row in reader:
            row['tag'] = row['tag'].translate({ord(k): None for k in '(,)'})
            rows.append(row)

    db.set_tags_list(rows)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.WARNING,
                        filename=Path(os.getcwd()) / f'{strftime("%Y-%m-%d_%H-%M-%S", gmtime())}.log', filemode='a')
    logger = logging.getLogger("dicom2sql")
    logger.setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

    args = parse_args()
    config = parse_config()

    db = Database(config["out_db_uri"])

    if args.init_db != '':
        upload_tags_description(args.init_db, db)

    project = None
    if args.project != '':
        project = db.get_or_create_project(args.project)

    inputs = list(map(lambda p: Path(p), args.paths))

    for path in inputs:
        file_extractor = FileExtractor(path)

        count = 0
        avg_dicom = 0
        avg_db = 0
        for file in file_extractor.files():
            with file:
                time_a = perf_counter_ns()
                if file.error:
                    continue
                print(file)
                time_b = perf_counter_ns()
                delta_dicom = time_b - time_a
                print(f"Time of getting next dicom: {delta_dicom}ns")

                community = path
                time_a = perf_counter_ns()
                try:
                    db.insert(file.dcm_data, str(community), str(file.path), project)
                except KeyError as e:
                    logger.error(f'missing tag {e.args[0]} in file {file}')
                except sqlalchemy.exc.ProgrammingError as e:
                    logger.error(f'exception occurred while inserting file {file}: {e}')
                time_b = perf_counter_ns()
                delta_db = time_b - time_a
                print(f"Time of updating db: {delta_db}ns")

                if count == 0:
                    count = 1
                    avg_dicom = delta_dicom
                    avg_db = delta_db
                else:
                    count += 1
                    avg_dicom = avg_dicom + (delta_dicom - avg_dicom) / count
                    avg_db = avg_db + (delta_db - avg_db) / count
            print(f"Average time of getting next DICOM: {avg_dicom}ns over {count} DICOMs")
            print(f"Average time of updating db: {avg_db}ns over {count} DICOMs")



