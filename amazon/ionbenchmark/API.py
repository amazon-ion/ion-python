from enum import Enum


# Serialization/deserialization APIs to benchmark.
class API(Enum):
    """Enumeration of the APIs."""
    LOAD_DUMP = 'load_dump'
    STREAMING = 'streaming'
    DEFAULT = 'load_dump'
