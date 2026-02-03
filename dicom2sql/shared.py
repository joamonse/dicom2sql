import argparse
import configparser
import os
import urllib.parse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser \
        (description='Run a dicom node that upload recied dicom\'s tags to a mongo database',
                                     prog='dicom2mongo')
    parser.add_argument('--init_db', default='', help='path to csv file containing the tags to upload')
    parser.add_argument('--project', default='', help='project to be assigned to the series')
    parser.add_argument('db_url', help='Database name')
    parser.add_argument('paths', help='Paths to folders containing the dicom files', nargs='*')

    return parser.parse_args()


def get_db_uri(db_config: configparser.SectionProxy, password:str) -> str:
    uri=db_config["type"]
    if db_config["type"].startswith("sqlite"):
        uri+=f":///{db_config['uri']}"
    else:
        uri += f"://{urllib.parse.quote_plus(db_config['username'])}:{urllib.parse.quote_plus(password)})@{db_config['uri']}"
    if db_config['port']:
        uri += f":{db_config['port']}"
    uri += f":{db_config['database']}"
    if db_config['options']:
        uri += f"?{db_config['options']}"
    return uri


def parse_config() -> dict:
    config = configparser.ConfigParser()
    config.read(["default.ini", "config.ini"])

    values = {
        'in_db_uri': get_db_uri(config['database.in'], os.environ['IN_PASSWORD']),
        'out_db_uri': get_db_uri(config['database.out'], os.environ['OUT_PASSWORD'])
    }

    return values