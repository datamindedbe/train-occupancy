import json
import urllib
from datetime import datetime, timedelta
from os import path
import requests

DATA = '../data/'
URL_ROOT = 'http://graph.spitsgids.be/connections/?departureTime='
STATIONS_URL = 'https://irail.be/stations/NMBS'
FEEDBACK_URL = 'https://gtfs.irail.be/nmbs/feedback/occupancy-until-20161029.newlinedelimitedjsonobjects'
START = datetime(2016, 8, 13, 15, 0, 0)
END = datetime(2016, 11, 1, 0, 0, 0)


def retrieve_schedule(url_root, start, end, folder):
    current = start
    while current < end:
        ts = current.strftime('%Y-%m-%dT%H-%M')
        url = url_root + urllib.quote(ts)
        response = requests.get(url)
        if response.ok:
            data = json.loads(response.content)
            filename = path.join(folder, ts + '.json')
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
        filename = path.join(folder, 'stations.json')
        with open(filename, 'w') as outfile:
            json.dump(data, outfile, indent=4)

def retrieve_feedback(feedback_url, folder):
    response = requests.get(feedback_url, verify=False)
    if response.ok:
        data = response.content
        filename = path.join(folder, 'feedback.ndjson')
        with open(filename, 'w') as outfile:
            outfile.write(data)


retrieve_schedule(URL_ROOT, start=START, end=END, folder=DATA)
retrieve_stations(STATIONS_URL, DATA)
retrieve_feedback(FEEDBACK_URL, DATA)
