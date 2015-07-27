__author__ = 'Mehmet Ahat'


import MapReduce
import sys



mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: word itself
    # value: document id

    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, key)


def reducer(key, list_of_values):
    # key: word
    # value: document ids

    mr.emit((key, list_of_values))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    #filePath = ".//data//books.json"
    #inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)