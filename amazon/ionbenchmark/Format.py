from enum import Enum


class Format(Enum):
    """Enumeration of the formats."""
    ION_TEXT = 'ion_text'
    ION_BINARY = 'ion_binary'
    DEFAULT = 'ion_binary'
