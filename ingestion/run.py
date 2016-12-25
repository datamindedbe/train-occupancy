from datetime import datetime

from config import CONNECTION_STRING

from ingestion.etl import Etl
from ingestion.ingest import ingest

FOLDER = '../data/'
URL_ROOT = 'http://graph.spitsgids.be/connections/?departureTime='
STATIONS_URL = 'https://irail.be/stations/NMBS'
FEEDBACK_URL = 'https://gtfs.irail.be/nmbs/feedback/occupancy-until-20161029.newlinedelimitedjsonobjects'
START = datetime(2016, 11, 1, 1, 0, 0)
END = datetime(2016, 12, 4, 0, 0, 0)

ingest(URL_ROOT, STATIONS_URL, FEEDBACK_URL, START, END, FOLDER)
etl = Etl(CONNECTION_STRING, FOLDER)
etl.run()

