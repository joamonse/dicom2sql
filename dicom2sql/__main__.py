import logging
import argparse
import os
from time import strftime, gmtime

import pydicom
import sqlalchemy
from pydicom.errors import InvalidDicomError

from dicom2sql.filesystem.file_lister import get_files_from_list
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
    logger.setLevel(logging.ERROR)
    logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

    parser = argparse.ArgumentParser(description='Run a dicom node that upload recied dicom\'s tags to a mongo database',prog='dicom2mongo')
    parser.add_argument('--init_db', default='', help='path to csv file containing the tags to upload')
    parser.add_argument('--project', default='', help='project to be assigned to the series')
    parser.add_argument('db_url', help='Database name')
    parser.add_argument('paths', help='Paths to folders containing the dicom files', nargs='*')

    args = parser.parse_args()

    db = Database(args.db_url)

    if args.init_db != '':
        upload_tags_description(args.init_db, db)

    project = None
    if args.project != '':
        project = db.get_or_create_project(args.project)

    inputs = list(map(lambda p: Path(p), args.paths))

    for path in inputs:
        file_extractor = FileExtractor(path)
        for file in file_extractor.files():
            with file:
                if file.error:
                    continue
                print(file)

                community = path
                try:
                    db.insert(file.dcm_data, str(community), str(file.path), project)
                except KeyError as e:
                    logger.error(f'missing tag {e.args[0]} in file {file}')
                except sqlalchemy.exc.ProgrammingError as e:
                    logger.error(f'exception occurred while inserting file {file}: {e}')



