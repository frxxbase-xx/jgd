# Run these tests (at top level)
#   nosetests
# To install nose:
#   easy_install nose

# First check for syntax errors
# TODO: relative import sucks
from ..jgd2 import TripleStore
import json

def test_basic_query():
    ts = TripleStore()

    # TODO: load JSON from file should be in TripleStore
    f = open('test1.jgd', "r") # TODO: relative path problem
    json_data = json.load(f)
    f.close()
    ts.load_json(json_data)
    
    #TODO: we should load this from disk
    result = ts.MQL([{"id":None,"knows":{"name":"Sue"}}])
    
    #TODO: we should load expected result from disk
    ids = [ person['id'] for person in result ]
    assert ids == [':bob', ':mary']
