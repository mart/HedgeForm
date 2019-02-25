import os


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
            file_map[col[1]] = col[2].replace("XXXX", "").replace("ZZZZ", "")   # SEC sometimes pads with extra letters
    del file_map['CUSIP']
    return file_map
