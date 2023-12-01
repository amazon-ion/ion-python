import json

class JsonLoadDump:

    def load(self, fp):
        for line in fp.readlines():
            yield json.loads(line)
