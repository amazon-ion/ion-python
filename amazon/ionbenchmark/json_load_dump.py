import json


class JsonLoadDump:
    @classmethod
    def load(self, fp):
        while True:
            line = fp.readline()
            if line == '':
                return
            yield json.loads(line)

    @classmethod
    def loads(self, s):
        return json.loads(s)

    @classmethod
    def dump(self, obj, fp):
        """
        The given obj must be a generator that holds all top-level objects
        """
        for v in obj:
            json.dump(v, fp)

    @classmethod
    def dumps(self, obj):
        for v in obj:
            json.dumps(v)
