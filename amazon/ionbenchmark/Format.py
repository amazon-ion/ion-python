import shutil
from enum import Enum
import amazon.ion.simpleion as simpleion
import os


def _file_is_ion_binary(file):
    return os.path.splitext(file)[1] == '.10n'


def _file_is_ion_text(file):
    return os.path.splitext(file)[1] == '.ion'


def format_is_ion(format_option):
    return (format_option == Format.ION_BINARY.value) or (format_option == Format.ION_TEXT.value)


def format_is_json(format_option):
    return (format_option == Format.JSON.value) or (format_option == Format.SIMPLEJSON.value) \
        or (format_option == Format.UJSON.value) or (format_option == Format.RAPIDJSON.value)


def format_is_cbor(format_option):
    return (format_option == Format.CBOR.value) or (format_option == Format.CBOR2.value)


def format_is_binary(format_option):
    return format_is_cbor(format_option) or (format_option == Format.ION_BINARY.value) \
           or (format_option == Format.PROTOBUF.value) or (format_option == Format.SD_PROTOBUF.value)


def rewrite_file_to_format(file, format_option):
    temp_file_name_base = 'temp_' + os.path.splitext(os.path.basename(file))[0]
    if format_option == Format.ION_BINARY.value:
        temp_file_name_suffix = '.10n'
    elif format_option == Format.ION_TEXT.value:
        temp_file_name_suffix = '.ion'
    else:
        temp_file_name_suffix = ''
    temp_file_name = temp_file_name_base + temp_file_name_suffix
    # Check the file path
    if os.path.exists(temp_file_name):
        os.remove(temp_file_name)

    if format_is_ion(format_option):
        # Write data if a conversion is required
        if (format_option == Format.ION_BINARY.value and _file_is_ion_text(file)) \
                or (format_option == Format.ION_TEXT.value and _file_is_ion_binary(file)):
            # Load data
            with open(file, 'br') as fp:
                obj = simpleion.load(fp, single_value=False)
            with open(temp_file_name, 'bw') as fp:
                if format_option == Format.ION_BINARY.value:
                    simpleion.dump(obj, fp, binary=True)
                else:
                    simpleion.dump(obj, fp, binary=False)
        else:
            shutil.copy(file, temp_file_name)
    else:
        return file

    return temp_file_name


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
    PROTOBUF = 'protobuf'
    SD_PROTOBUF = 'self_describing_protobuf'
