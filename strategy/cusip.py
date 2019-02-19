import os
from pymongo import MongoClient


def update_cusip_map():
    client = MongoClient(os.environ['MONGO'])
    db = client.form13f
    cusip_map = {}
    directory = os.environ["TEXT_FILES_DIR"]
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".txt"):
            file = os.path.join(directory, filename)
            # SEC specified ascii but occasional non-ascii appear anyway
            with open(file, encoding="ASCII", errors="ignore") as f:
                cusip_map.update(parse_fail_filing(f))
    db.cusipmap.insert_one(cusip_map)


def parse_fail_filing(file):
    cusip_map = {}
    for line in file:
        col = line.split('|')
        if len(col) > 2:
            cusip_map[col[1]] = col[2].replace("XXXX", "").replace("ZZZZ", "")
    del cusip_map['CUSIP']
    return cusip_map
