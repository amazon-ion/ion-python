from enum import Enum
import amazon.ion.simpleion as simpleion
import os

temp_file = 'temp_file'


def format_is_ion(format_option):
    return (format_option == Format.ION_BINARY.value) or (format_option == Format.ION_TEXT.value)


def format_is_json(format_option):
    return (format_option == Format.JSON.value) or (format_option == Format.SIMPLEJSON.value) \
        or (format_option == Format.UJSON.value) or (format_option == Format.RAPIDJSON.value)


def format_is_cbor(format_option):
    return (format_option == Format.CBOR.value) or (format_option == Format.CBOR2.value)


def format_is_binary(format_option):
    return format_is_cbor(format_option) or (format_option == Format.ION_BINARY.value)


def rewrite_file_to_format(file, format_option):
    with open(file, 'br') as fp:
        obj = simpleion.load(fp, single_value=False)
    if os.path.exists(temp_file):
        os.remove(temp_file)
    if format_is_ion(format_option):
        with open(temp_file, 'bw') as fp:
            if format_option == Format.ION_BINARY.value:
                simpleion.dump(obj, fp, binary=True)
            elif format_option == Format.ION_TEXT.value:
                simpleion.dump(obj, fp, binary=False)
        file = temp_file
    return file


class Format(Enum):
    """Enumeration of the formats."""
    ION_TEXT = 'ion_text'
    ION_BINARY = 'ion_binary'
    JSON = 'json'
    SIMPLEJSON = 'simplejson'
    UJSON = 'ujson'
    RAPIDJSON = 'rapidjson'
    CBOR = 'cbor'
    CBOR2 = 'cbor2'
    DEFAULT = 'ion_binary'
