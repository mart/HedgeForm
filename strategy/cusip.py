import os

from pymongo import MongoClient

client = MongoClient(os.environ['MONGODB_URI'])
db = client.get_database()


def create_cusip_map(directory):
    cusip_map = {}
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            file = os.path.join(directory, filename)
            # SEC specified ascii but occasional non-ascii appear anyway
            with open(file, encoding="ASCII", errors="ignore") as f:
                cusip_map.update(parse_fail_filing(f))
    return cusip_map


def parse_fail_filing(file):
    file_map = {}
    for line in file:
        col = line.split('|')
        if len(col) > 2:
            file_map[col[1]] = col[2].replace("XXXX", "").replace("ZZZZ", "")  # SEC sometimes pads with extra letters
    del file_map['CUSIP']
    return file_map


def update_cusip():
    cusip_map = db.cusipmap.find_one()
    cusip_map.update(create_cusip_map(os.environ["CUSIP_DIR"]))
    db.cusipmap.replace_one({}, cusip_map, upsert=True)
