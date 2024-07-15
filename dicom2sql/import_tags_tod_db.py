#based on https://gist.github.com/jxub/f722e0856ed461bf711684b0960c8458

import pandas as pd
from pymongo import MongoClient
import json

def mongoimport(tsv_path, db_name, coll_name, db_url='localhost', db_port=27017):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv(tsv_path, sep="\t")
    data["TAGS"] = data["TAGS"].apply(lambda s: s.translate( { ord(i): None for i in '(,)'} ))
    payload = json.loads(data.to_json(orient='records'))
    coll.drop()
    x = coll.insert_many(payload)
    return len(x.inserted_ids)

if __name__ == '__main__':
    import argparse
    import pathlib

    parser = argparse.ArgumentParser(description='Upload csv to mongodb')
    parser.add_argument('file', type=pathlib.Path, help='path to tsv file')
    parser.add_argument('dbname', help='Database name')
    parser.add_argument('collection_name', help='Table name')

    args = parser.parse_args()
    if args.file.exists():
        print(f'inserted {mongoimport(args.file, args.dbname, args.collection_name)}')
    else:
        print(f'File {args.file} does not exists')
