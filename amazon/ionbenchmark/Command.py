from enum import Enum


class Command(Enum):
    """Enumeration of the Command."""
    READ = 'read'
    WRITE = 'write'
    COMPARE = 'compare'
