from enum import Enum


class Io_type(Enum):
    """Enumeration of the IO types."""
    FILE = 'file'
    BUFFER = 'buffer'
    DEFAULT = 'file'
