import json
from collections import namedtuple


def read_json(file):
    with open(file) as json_file:
        data = json.load(json_file)
        data_as_string = json.dumps(data)
        return json_to_obj(data_as_string)


def json_to_obj(data): return json.loads(data, object_hook=__json_object_hook)


def __json_object_hook(d): return namedtuple('', d.keys())(*d.values())
