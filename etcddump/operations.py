from urlparse import urlparse
import etcd
import json
import sys
import os

reload(sys)
sys.setdefaultencoding('utf8')


class BaseOperations(object):

    def __init__(self, url='http://localhost:4001'):
        self.get_client(url)


    def get_client(self, url,cert=None):
        parsed = urlparse(url)
        (h, p) = parsed.netloc.split(':')

        # SSL certificate, env variables
        if parsed.scheme == 'https':
            cert = (os.getenv('ETCD_SSL_CER'),os.getenv('ETCD_SSL_KEY'))

        self.client = etcd.Client(host=h, port=int(p), protocol=parsed.scheme, allow_reconnect=False, cert=cert)

    def entry_from_result(self, entry):
        return {
            'key': entry.key,
            'value': entry.value,
            'ttl': entry.ttl,
            'dir': entry.dir,
            'index': entry.modifiedIndex
        }


class Dumper(BaseOperations):


    def dump(self, filename=None):
        data = self.client.read('/', recursive=True)
        d = {}
        for entry in data.children:
            d[entry.modifiedIndex] = self.entry_from_result(entry)

        indexes = sorted(d.keys())
        dumplist = []
        for idx in indexes:
            dumplist.append(d[idx])

        if filename:
            with open(filename, 'w') as f:
                json.dump(dumplist,f)
        else:
            print(json.dumps(dumplist))

class Restorer(BaseOperations):

    def restore(self, filename=None):
        if filename:
            with open(filename, 'rb') as f:
                data = json.load(f)
        else:
            with sys.stdin as f:
                data = json.load(f)

        for entry in data:
            r = self.write(entry)

    def write(self, entry):
        return self.client.write(entry['key'], entry['value'], ttl = entry['ttl'], dir = entry['dir'])
