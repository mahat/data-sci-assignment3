__author__ = 'Mehmet Ahat'


import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: ids
    # value: record itself
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: no need to key for emit because just joins of records are enough
    # value: join values
    order_list = []
    lineItem_list = []

    for elem in list_of_values:
        if elem[0] == 'order':
            order_list.append(elem)
        if elem[0] == 'line_item':
            lineItem_list.append(elem)


    for order in order_list:
        for lineItem in lineItem_list:
            join = order + lineItem
            mr.emit(join)

# Do not modify below this line
# =============================
if __name__ == '__main__':
    #filePath = ".//data//records.json"
    #inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)