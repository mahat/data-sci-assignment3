__author__ = 'Mehmet Ahat'

import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: person
    # value: friend of the person
    key1 = record[0]
    key2 = record[1]
    mr.emit_intermediate(key1, key2)
    mr.emit_intermediate(key2, key1)


def reducer(key, list_of_values):
    # key: person
    # value: people who have asymmetric friendship
    singles = [x for x in list_of_values if list_of_values.count(x) == 1]
    for x in singles:
        mr.emit((key, x))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    # filePath = ".//data//friends.json"
    # inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
