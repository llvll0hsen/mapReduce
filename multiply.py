import MapReduce
import sys

"""
    Word Count Example in the Simple Python MapReduce Framework
    """

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #matrix_key: matrix a or b
    # value: document contents
    matrix_key = record[0]
    value = record
    if matrix_key == 'a':
        for k in range(5):
            i = int(record[1])
            key = (i,k)
            mr.emit_intermediate(key,record)
    if matrix_key == 'b':
        for i in range(5):
            k = int(record[2])
            key =(i,k)
            mr.emit_intermediate(key,record)
        

def reducer(key, list_of_values):
    a = [0]*5
    b = [0]*5
    for entry in list_of_values:
        i = entry[1]
        j = entry[2]
        value = entry[3]
        if entry[0]=='a':
            a[j] = value
        if entry[0]=='b':
            b[i] = value
    val = sum(p*q for p,q in zip(a, b))
    
    mr.emit((key[0],key[1],val))

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
