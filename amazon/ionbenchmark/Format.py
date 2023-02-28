from enum import Enum


def format_is_ion(format_option):
    return (format_option == Format.ION_BINARY.value) or (format_option == Format.ION_TEXT.value)


def format_is_json(format_option):
    return (format_option == Format.JSON.value) or (format_option == Format.SIMPLEJSON.value) \
           or (format_option == Format.UJSON.value) or (format_option == Format.RAPIDJSON.value) or \
           (format_option == Format.ORJSON.value)


def format_is_cbor(format_option):
    return (format_option == Format.CBOR.value) or (format_option == Format.CBOR2.value)


def format_is_binary(format_option):
    return format_is_cbor(format_option) or (format_option == Format.ION_BINARY.value) \
           or (format_option == Format.ORJSON.value)


def rewrite_file_to_format(file, format_option):
    return file


class Format(Enum):
    """Enumeration of the formats."""
    ION_TEXT = 'ion_text'
    ION_BINARY = 'ion_binary'
    JSON = 'json'
    SIMPLEJSON = 'simplejson'
    UJSON = 'ujson'
    RAPIDJSON = 'rapidjson'
    ORJSON = 'orjson'
    CBOR = 'cbor'
    CBOR2 = 'cbor2'
    DEFAULT = 'ion_binary'

