import freebase
import json
import sys
from pprint import pprint as pp

f = open(sys.argv[1], "r")
q = json.load(f)
f.close()
it = freebase.mqlreaditer(q)
print "["
for i in range(int(sys.argv[2])):
    out = it.next()
    print json.dumps(out, indent=2)
    if i != int(sys.argv[2]) - 1:
        print ","
    
print "]"
