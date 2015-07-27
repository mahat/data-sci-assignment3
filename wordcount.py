import MapReduce
import sys



mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: word itself
    # value: word occurrence

    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, 1)


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    #filePath = ".//data//books.json"
    #inputdata = open(filePath)
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
