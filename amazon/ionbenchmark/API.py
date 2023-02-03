from enum import Enum


# Serialization/deserialization APIs to benchmark.
class API(Enum):
    """Enumeration of the APIs."""
    SIMPLE_ION = 'simple_ion'
    EVENT = 'event'
    DEFAULT = 'simple_ion'
