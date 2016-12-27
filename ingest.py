import argparse
from datetime import datetime, timedelta

import dateutil.relativedelta
from config import CONNECTION_STRING

from ingestion.etl import Etl
from ingestion.ingest import Ingest

CONNECTIONS_URL = 'http://graph.spitsgids.be/connections/?departureTime='
STATIONS_URL = 'https://irail.be/stations/NMBS'
FEEDBACK_URL = 'https://gtfs.irail.be/nmbs/feedback/occupancy-until-20161029.newlinedelimitedjsonobjects'


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser(description='Parse ingest options')
# Switch
parser.add_argument('-w', '--wipe', action='store_const', const=True,
                    help='Wipe the database. Will drop all tables. Default is FALSE')
parser.add_argument('-f', '--forceIngest', action='store_const', const=True,
                    help="Don't skip existing ingest files. Default is FALSE")
parser.add_argument('-s', "--startDate", required=False, type=valid_date,
                    help="The Start Date - format YYYY-MM-DD. Default is 1 month ago.")
parser.add_argument('-e', "--endDate", required=False, type=valid_date,
                    help="The End Date - format YYYY-MM-DD. Default is now()")
parser.add_argument('-o', "--outputFolder", required=False,
                    help="The folder in which to store the files. Default is 'data/'")

args = parser.parse_args()

if args.endDate is not None:
    END = args.endDate
else:
    END = datetime.now()
    END = END - timedelta(minutes=END.minute % 10, seconds=END.second, microseconds=END.microsecond)
if args.startDate is not None:
    START = args.startDate
else:
    START = END - dateutil.relativedelta.relativedelta(months=1)

WIPE = args.wipe if args.wipe is not None else False
FOLDER = args.outputFolder if args.outputFolder is not None else 'data'
FORCE_INGEST = args.forceIngest if args.forceIngest is not None else False

print "Ingesting from %s to %s. Initialize=%s" % (START, END, WIPE)
ingest = Ingest(CONNECTIONS_URL, STATIONS_URL, FEEDBACK_URL, START, END, FOLDER, FORCE_INGEST)
ingest.run()
etl = Etl(CONNECTION_STRING, FOLDER, WIPE)
etl.run()
