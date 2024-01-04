import json


class JsonLoadDump:
    @classmethod
    def load(self, fp):
        while True:
            line = fp.readline()
            if line == '':
                break
            yield json.loads(line)

    @classmethod
    def loads(self, s):
        json.loads(s)

    @classmethod
    def dump(self, obj, fp):
        json.dump(obj, fp)

    @classmethod
    def dumps(self, obj):
        json.dumps(obj)
