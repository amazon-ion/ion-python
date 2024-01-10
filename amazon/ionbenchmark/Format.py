import shutil
from enum import Enum
import amazon.ion.simpleion as simpleion
import os


def _file_is_ion_binary(file):
    return os.path.splitext(file)[1] == '.10n'


def _file_is_ion_text(file):
    return os.path.splitext(file)[1] == '.ion'


def format_is_ion(format_option):
    return format_option in (Format.ION_BINARY, Format.ION_TEXT)


def format_is_json(format_option):
    return format_option in (Format.JSON, Format.SIMPLEJSON, Format.UJSON, Format.RAPIDJSON)


def format_is_cbor(format_option):
    return format_option in (Format.CBOR, Format.CBOR2)


def format_is_binary(format_option):
    return format_option in (Format.ION_BINARY, Format.PROTOBUF, Format.SD_PROTOBUF, Format.CBOR, Format.CBOR2)


def rewrite_file_to_format(file, format_option):
    temp_file_name_base = 'temp_' + os.path.splitext(os.path.basename(file))[0]
    if format_option is Format.ION_BINARY:
        temp_file_name_suffix = '.10n'
    elif format_option is Format.ION_TEXT:
        temp_file_name_suffix = '.ion'
    else:
        temp_file_name_suffix = ''
    temp_file_name = temp_file_name_base + temp_file_name_suffix
    # Check the file path
    if os.path.exists(temp_file_name):
        os.remove(temp_file_name)

    # todo fix format_is_ checks to identity compared to enum constant
    if format_is_ion(format_option):
        # Write data if a conversion is required
        if (format_option is Format.ION_BINARY and _file_is_ion_text(file)) \
                or (format_option is Format.ION_TEXT and _file_is_ion_binary(file)):
            # Load data
            with open(file, 'br') as fp:
                obj = simpleion.load(fp, single_value=False)
            with open(temp_file_name, 'bw') as fp:
                if format_option is Format.ION_BINARY:
                    simpleion.dump(obj, fp, binary=True, sequence_as_stream=True)
                else:
                    simpleion.dump(obj, fp, binary=False, sequence_as_stream=True)
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

    @staticmethod
    def by_value(value):
        for e in Format:
            if e.value == value:
                return e
        raise ValueError(f"No enum constant with value {value}")
