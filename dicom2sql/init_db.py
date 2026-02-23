import argparse
import csv
import logging
import os
from pathlib import Path
from time import strftime, gmtime

from dicom2sql.shared import parse_config
from dicom2sql.sql.database import Database


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
    logger.setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

    parser = argparse.ArgumentParser \
        (description='Init tag list',
         prog='dcm2sql')
    parser.add_argument('tag_list', default='', help='path to csv file containing the tags to upload')

    args = parser.parse_args()

    config = parse_config()

    db_out = Database(config["out_db_uri"])

    upload_tags_description(args.tag_list, db_out)