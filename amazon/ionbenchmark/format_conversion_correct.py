import json
import os

import cbor_json

from amazon.ion.json_encoder import IonToJSONEncoder
from amazon.ion.simpleion import load, dump

# The file names you want to convert or be converted
ion_file = 'hkg'
json_file = 'hkg_json'
cbor_file = 'hkg_cbor'

# Flags to control whether a file in a specific format needs to be generated.
convert_json = True
convert_cbor = True

# Flag to control if the JSON file is delimited by a newline.
generate_jsonl = True

with open(ion_file, 'br') as fp:
    if convert_json:
        # return an iterator
        it = load(fp, single_value=False, parse_eagerly=False)
        if os.path.exists(json_file):
            os.remove(json_file)
        with open(json_file, 'a') as fp2:
            while True:
                try:
                    # get the current Ion value
                    value = next(it)
                    # Convert Ion value to Json string
                    json.dump(value, fp2, cls=IonToJSONEncoder)
                    if generate_jsonl:
                        fp2.writelines("\n")
                except StopIteration:
                    break

    if convert_cbor:
        fp.seek(0)
        it = load(fp, single_value=False, parse_eagerly=False)
        if os.path.exists(cbor_file):
            os.remove(cbor_file)
        with open(cbor_file, 'ba') as fp3:
            while True:
                try:
                    # Get the Ion value
                    value = next(it)
                    # Dumps Ion value to Json string
                    json_str = json.dumps(value, cls=IonToJSONEncoder)
                    # Loads JSON string as python objects
                    j = json.loads(json_str)
                    # Convert Json to cbor
                    c = cbor_json.cbor_from_native(j)
                    fp3.write(c)
                except StopIteration:
                    break
