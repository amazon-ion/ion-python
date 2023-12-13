import json
import os

import cbor_json

from amazon.ion.json_encoder import IonToJSONEncoder
from amazon.ion.simpleion import load

# The file names you want to convert or be converted
ion_file = 'test_ion'
json_file_list = 'test_json'
cbor_file_list = 'test_cbor'

# Flags to control whether a file in a specific format needs to be generated.
convert_json = True
convert_cbor = True

# Flag to control if the JSON file is delimited by a newline.
generate_jsonl = True

with open(ion_file, 'br') as fp:
    if convert_json:
        it = load(fp, single_value=False)
        if os.path.exists(json_file_list):
            os.remove(json_file_list)
        with open(json_file_list, 'w') as fp2:
            json.dump(it, fp2, cls=IonToJSONEncoder)

    if convert_cbor:
        fp.seek(0)
        i = load(fp, single_value=False)
        # ion<>json conversion
        j = json.dumps(i, cls=IonToJSONEncoder)
        # json<>cbor conversion
        if os.path.exists(cbor_file_list):
            os.remove(cbor_file_list)
        with open(cbor_file_list, 'bw') as fp3:
            c = cbor_json.cbor_from_jsonable(j)
            fp3.write(c)



