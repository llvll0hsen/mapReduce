import MapReduce
import json
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    #removing indentical words
    words = list(set(words))
    for w in words:
        mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    mr.emit((key, list_of_values))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

