import json
import os
import time

from amazon.ion import simpleion
from amazon.ion.json_encoder import IonToJSONEncoder

# iterations
count = 100
# number of top-level values within the document
obj_count = 2000
# File names
list_file = "json_big_list"
doc_file = "json_top_level_top"
# each top-level object
obj = simpleion.loads('''{record_id:13036606394910245365,record_version:"2021-11-06T09:20:47.558000",owner:"John Sanders",name:"Boomer Sanders",breed:'German Wirehaired Pointer',vaccines:[Adenovirus,Bordetella],weight:62d0,birthdate:"2010-03-24T00:00:00"} ''' * 10, single_value=False)

# 1. Generate test files
# Generates a JSON list that holds all objects
if os.path.exists(list_file):
    os.remove(list_file)
with open(list_file, 'w') as fp_1:
    l = [obj] * obj_count
    json.dump(l, fp_1, cls=IonToJSONEncoder)
# Generate JSON document that includes all objects as the top-level objects
if os.path.exists(doc_file):
    os.remove(doc_file)
with open(doc_file, 'a') as fp_2:
    for i in range(obj_count):
        json.dump(obj, fp_2, cls=IonToJSONEncoder)
        fp_2.writelines("\n")

# 2. Benchmarking
# Json - read objects within a list
start = time.time()
for i in range(count):
    with open(list_file, 'r') as fp:
        json.load(fp)
print('json previous list:', time.time() - start)

# Json - read each line
start = time.time()
for i in range(count):
    with open(doc_file, 'r') as fp:
        for jsonL in fp.readlines():
            json.loads(jsonL)
print('json new readline:', time.time() - start)

# Sample result
#
# count = 100, object_count = 2000, obj = {'a': 2}
# json previous list: 0.023343801498413086
# json new readline: 0.1807858943939209
#
# count = 100, object_count = 2000, obj = simpleion.loads('''{record_id:13036606394910245365,record_version:"2021-11-06T09:20:47.558000",owner:"John Sanders",name:"Boomer Sanders",breed:'German Wirehaired Pointer',vaccines:[Adenovirus,Bordetella],weight:62d0,birthdate:"2010-03-24T00:00:00"} ''' * 10, single_value=False)
# json previous list: 3.009171962738037
# json new readline: 2.4911370277404785
#
# Conclusion: So the results depend on the number of top-level values and the complexity of the objects.








