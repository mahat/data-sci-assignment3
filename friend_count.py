__author__ = 'Mehmet Ahat'

import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: person name
    # value: friend count
    key = record[0]
    mr.emit_intermediate(key, 1)


def reducer(key, list_of_values):
    # key: person name
    # value: total number of friend counts
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    #filePath = ".//data//friends.json"
    #inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
