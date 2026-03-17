import csv
import logging
import os
import smtplib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from email.message import EmailMessage
from pathlib import Path
from time import strftime, gmtime, perf_counter_ns

import pydicom
import sqlalchemy
from pydicom.errors import InvalidDicomError

from dicom2sql.shared import parse_config
from dicom2sql.sql.database import Database, get_image_paths, delete_image_paths


def send_daily_email(processed_count):
    msg = EmailMessage()
    msg["Subject"] = f"Daily Processing Summary ({date.today()})"
    msg["From"] = "noreply@example.com"
    msg["To"] = "you@example.com"

    msg.set_content(f"Total items processed today: {processed_count}")

    # Example SMTP (replace with your provider)
    with smtplib.SMTP("smtp.example.com", 587) as server:
        server.starttls()
        server.login("user", "password")
        server.send_message(msg)


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

#    args = parse_args()
    config = parse_config()
    print(config['database.out']['out_db_uri'])
    db_out = Database(config['database.out']['out_db_uri'], pool_size=int(config["server"]["threads"])+1)
    def process_file(path:str) -> int:
        logging.getLogger("dicom2sql").debug(f'Opening {path}')
        error=0
        try:
            dcm_data = pydicom.dcmread(path, stop_before_pixels=True)
        except (InvalidDicomError,):
            logging.getLogger("dicom2sql").error(f'{path} contains error or is not a dicom')
            return 1

        except TypeError:
            logging.getLogger("dicom2sql").warning(f'{path} is not dicom')
            return 2

        except (FileNotFoundError , OSError):
            logging.getLogger("dicom2sql").warning(f'{path} does not exist')
            return 3

        logging.getLogger("dicom2sql").debug(f'uploading {path}')

        community = path
        try:
            db_out.insert(dcm_data, str(community), str(path))
        except KeyError as e:
            logger.error(f'missing tag {e.args[0]} in file {path}')
            return 4
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as e:
            logger.error(f'exception occurred while inserting file {path}: {e}')
            return 5
        logging.getLogger("dicom2sql").debug(f'uploading {path} completed')

        return 0

#    if args.init_db != '':
#        upload_tags_description(args.init_db, db_out)


    current_day = date.today()
    processed_today = 0

    while True:
        time_a = perf_counter_ns()
        data = db_out.get_new_images()
        if not data:
            data = db_out.get_new_images(filter_error=3)
        print(data)

        # daily rollover check
        if date.today() != current_day:
            #send_daily_email(processed_today)
            processed_today = 0
            current_day = date.today()

        if not data:
            time.sleep(int(config["server"]["wait"])*60)
            continue

        with ThreadPoolExecutor(max_workers=int(config["server"]["threads"])) as pool:
            results = pool.map(process_file, [d[1] for d in data])

        mapped_results = [(d[0],c) for d,c in zip(data,results)]
        ok, fail = db_out.update_new_images(mapped_results)
        logging.getLogger("dicom2sql").info(f'Completed {ok+fail} images. Failed {fail}. Correct {ok}')

        print(f"Completed {ok+fail} images. Failed {fail}. Correct {ok}")
        time_b = perf_counter_ns()
        delta_db = time_b - time_a
        print(f"Time of updating db: {delta_db}ns")
