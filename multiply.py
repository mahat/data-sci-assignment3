__author__ = 'Mehmet Ahat'

import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
num_cols = 5
num_rows = 5


def mapper(record):
    # key: index of matrix which will be result of multiplication of a and b
    # string are user for key values in order to simplify the logic
    # value: records
    mat = record[0]
    i = record[1]
    j = record[2]

    if mat == 'a':
        for x in range(0, num_cols):
            key = str(i) + ',' + str(x)
            mr.emit_intermediate(key, record)

    if mat == 'b':
        for x in range(0, num_rows):
            key = str(x) + ',' + str(j)
            mr.emit_intermediate(key, record)


def reducer(key, list_of_values):
    # key: index of result matrix
    # value: multiplication result
    a_list = [x for x in list_of_values if x[0] == 'a']
    b_list = [x for x in list_of_values if x[0] == 'b']

    total = 0
    for a in a_list:
        for b in b_list:
            if a[2] == b[1]:
                total += a[3] * b[3]

    key_in = key.split(',')
    mr.emit((int(key_in[0]), int(key_in[1]), total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    # filePath = "matrix.json"
    # inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
