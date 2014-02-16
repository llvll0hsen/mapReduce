import MapReduce
import sys
import collections

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: name
    # value: document contents
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key, value)
    mr.emit_intermediate(value, key)

#mr.emit_intermediate(value, key)
def reducer(key, list_of_friends):
    # key: word
    # value: list of occurrence counts
    for friend in list_of_friends:
        if list_of_friends.count(friend) == 1:
            mr.emit((key,friend))
# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
