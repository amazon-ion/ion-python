import json


class JsonLoadDump:
    def __init__(self):
        pass

    def load(self, fp):
        while True:
            line = fp.readline()
            if line == '':
                return
            yield json.loads(line)

    def loads(self, s):
        return json.loads(s)

    def dump(self, obj, fp):
        """
        The given obj must be a generator that holds all top-level objects
        """
        for v in obj:
            json.dump(v, fp)

    def dumps(self, obj):
        for v in obj:
            json.dumps(v)
