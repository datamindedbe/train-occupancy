import json
import os
import urllib
from datetime import timedelta
from os import path

import requests


class Ingest(object):
    def __init__(self, connections_url, stations_url, feedback_url, start, end, folder, force_ingest=False):
        self.connections_url = connections_url
        self.stations_url = stations_url
        self.feedback_url = feedback_url
        self.start = start
        self.end = end
        self.folder = folder
        self.force_ingest = force_ingest

    def retrieve_schedule(self):
        current = self.start
        while current < self.end:
            ts = current.strftime('%Y-%m-%dT%H-%M')
            filename = path.join(self.folder, 'connections', ts + '.json')
            if not self.force_ingest and os.path.isfile(filename):
                print "file %s exists, skipping..." % filename
            else:
                url = self.connections_url + urllib.quote(ts)
                response = requests.get(url)
                if response.ok:
                    data = json.loads(response.content)
                    with open(filename, 'w') as outfile:
                        json.dump(data, outfile, indent=4)
                    print '%s stored' % url
                else:
                    print '%s failed' % url

            current = current + timedelta(minutes=10)

    def retrieve_stations(self):
        filename = path.join(self.folder, 'stations', 'stations.json')
        if not self.force_ingest and os.path.isfile(filename):
            print "file %s exists, skipping..." % filename
        else:
            response = requests.get(self.stations_url, headers={'accept': 'application/json'}, verify=False)
            if response.ok:
                data = json.loads(response.content)
                with open(filename, 'w') as outfile:
                    json.dump(data, outfile, indent=4)

    def retrieve_feedback(self):
        filename = path.join(self.folder, 'feedback', 'feedback.ndjson')
        if not self.force_ingest and os.path.isfile(filename):
            print "file %s exists, skipping..." % filename
        else:
            response = requests.get(self.feedback_url, verify=False)
            if response.ok:
                data = response.content
                with open(filename, 'w') as outfile:
                    outfile.write(data)

    def create_folders(self):
        for folder in [path.join(self.folder, 'connections'),
                       path.join(self.folder, 'stations'),
                       path.join(self.folder, 'feedback')]:
            if not os.path.exists(folder):
                os.makedirs(folder)

    def run(self):
        self.create_folders()
        self.retrieve_schedule()
        self.retrieve_stations()
        self.retrieve_feedback()
