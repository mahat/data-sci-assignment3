__author__ = 'Mehmet Ahat'

import MapReduce
import sys



mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: trimmed genes
    # value: sequence id
    gene = record[1]
    key = gene[:-10]
    mr.emit_intermediate(key, record[0])


def reducer(key, list_of_values):
    # key: trimmed genes
    # value: no values is needed

    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
    #filePath = ".//data//dna.json"
    #inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
