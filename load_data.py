import csv
import datetime
import json
import sys
import hashlib

import pyelasticsearch as es


def strip_ms(time):
    if len(time) == 0:
        return None
    else:
        return int(time.replace(".", "")[:-3])


def prepare_line(line):
    status, ts, time, desc, location = line
    _id = hashlib.md5(str(line)).hexdigest()
    ts = datetime.datetime.strptime(ts, "%d/%m/%Y  %H:%M:%S").strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    record = {
        "id": _id,
        "status": status,
        "@timestamp": ts,
        "time": strip_ms(time),
        "location": location,
        "desc": desc,
    }
    return record

def mapping(es_type="string"):
    return  {
        "type": es_type,
        "index": "not_analyzed",
        "norms": {
            "enabled": False
        },
        "fielddata": {
            # "format": "disabled"
        },
        "fields": {
            "raw": {
                "type": es_type,
                "index": "not_analyzed",
                "ignore_above": 256
            }
        }
    }

class Indexer(object):
    BATCH_SIZE = 100
    def __init__(self, settings=None):
        self.settings = settings or {
            "number_of_shards": 1, "number_of_replicas": 0}
        self.client = es.ElasticSearch(urls=["http://localhost:9200"])
        self.index_name = None
        self._buffer = []

    def create_index(self):
        try:
            self.client.create_index(self.index_name, self.settings)
        except:
            pass
        self.client.put_mapping(
            self.index_name,
            "logs", {
                "logs": {
                    "properties": {
                        # "@timestamp": {"type": "integer"},
                        "status": mapping(),
                        "time":  {"type": "integer"},
                        "location":  mapping(),
                        "desc":  mapping(),
                    },
                }
            })
        return self

    def flush_buffer(self):
        if len(self._buffer) == 0:
            return
        self.client.bulk_index(self.index_name, "logs", self._buffer)
        self._buffer = []

    def index(self, event):
        day = event["@timestamp"][:10].replace("-", ".")
        if self.index_name != "logstash-" + day:
            self.flush_buffer()
            self.index_name = "logstash-" + day
            self.create_index()
        self._buffer.append(event)
        if len(self._buffer) >= self.BATCH_SIZE:
            self.flush_buffer()


def main():
    reader = csv.reader(sys.stdin)
    indexer = Indexer()
    for line in reader:
        event = prepare_line(line)
        indexer.index(event)
    indexer.flush_buffer()


if __name__ == '__main__':
    main()
