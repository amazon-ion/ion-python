import cbor2


class CborLoadDump:
    """
    Wrapper for simpleion API that holds some configuration so that the configuration can be encapsulated here instead
    of having to be plumbed through the whole benchmark code.

    Results of profiling indicate that this adds a trivial amount of overhead, even for small data. If Ion Python
    performance improves by >1000% from June 2023, then this may need to be re-evaluated.
    """

    @classmethod
    def load(self, fp):
        while True:
            try:
                yield cbor2.load(fp)
            except EOFError:
                return

    @classmethod
    def loads(self, s):
        cbor2.loads(s)

    @classmethod
    def dump(self, obj, fp):
        cbor2.dump(obj, fp)

    @classmethod
    def dumps(self, obj):
        cbor2.dumps(obj)