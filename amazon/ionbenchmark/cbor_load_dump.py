import cbor

class CborLoadDump:
    """
    Wrapper for simpleion API that holds some configuration so that the configuration can be encapsulated here instead
    of having to be plumbed through the whole benchmark code.

    Results of profiling indicate that this adds a trivial amount of overhead, even for small data. If Ion Python
    performance improves by >1000% from June 2023, then this may need to be re-evaluated.
    """

    def load(self, fp):
        while True:
            try:
                yield cbor.load(fp)
            except EOFError:
                return
