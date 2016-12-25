import json
import urllib
from datetime import timedelta
from os import path

import requests


def retrieve_schedule(url_root, start, end, folder):
    current = start
    while current < end:
        ts = current.strftime('%Y-%m-%dT%H-%M')
        url = url_root + urllib.quote(ts)
        response = requests.get(url)
        if response.ok:
            data = json.loads(response.content)
            filename = path.join(folder, 'connections', ts + '.json')
            with open(filename, 'w') as outfile:
                json.dump(data, outfile, indent=4)
            print '%s stored' % url
        else:
            print '%s failed' % url

        current = current + timedelta(minutes=10)


def retrieve_stations(stations_url, folder):
    response = requests.get(stations_url, headers={'accept': 'application/json'}, verify=False)
    if response.ok:
        data = json.loads(response.content)
        filename = path.join(folder, 'stations', 'stations.json')
        with open(filename, 'w') as outfile:
            json.dump(data, outfile, indent=4)


def retrieve_feedback(feedback_url, folder):
    response = requests.get(feedback_url, verify=False)
    if response.ok:
        data = response.content
        filename = path.join(folder, 'feedback', 'feedback.ndjson')
        with open(filename, 'w') as outfile:
            outfile.write(data)


def ingest(url_root, stations_url, feedback_url, start, end, folder):
    retrieve_schedule(url_root, start=start, end=end, folder=folder)
    retrieve_stations(stations_url, folder)
    retrieve_feedback(feedback_url, folder)
