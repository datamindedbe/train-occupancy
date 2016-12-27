import json
import os
from os import path

from datetime import datetime
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class Etl(object):
    def __init__(self, connection_string, folder, wipe=False):
        self.wipe = wipe
        self.connection_string = connection_string
        self.folder = folder

    def create_tables(self):
        with connect(self.connection_string) as con:
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                query = """
                    DROP TABLE IF EXISTS connection CASCADE;
                    CREATE TABLE connection(
                      departureStop VARCHAR(255) NOT NULL,
                      departureTime TIMESTAMP NOT NULL,
                      departureDate VARCHAR(255) NOT NULL,
                      departureDelay FLOAT NOT NULL,
                      arrivalStop VARCHAR(255) NOT NULL,
                      arrivalTime TIMESTAMP NOT NULL,
                      arrivalDate VARCHAR(255) NOT NULL,
                      arrivalDelay FLOAT NOT NULL,
                      route VARCHAR(255) NOT NULL,
                      trip VARCHAR(255) NOT NULL,
                      id VARCHAR(255) NOT NULL PRIMARY KEY
                    );

                    DROP TABLE IF EXISTS station CASCADE;
                    CREATE TABLE station(
                      name VARCHAR(255) NOT NULL,
                      country VARCHAR(255) NOT NULL,
                      longitude FLOAT NOT NULL,
                      latitude FLOAT NOT NULL,
                      id VARCHAR(255) NOT NULL PRIMARY KEY
                    );

                    DROP TABLE IF EXISTS occupancy CASCADE;
                    CREATE TABLE occupancy(
                      timestamp TIMESTAMP NOT NULL,
                      connection VARCHAR(255) NOT NULL,
                      stationFrom VARCHAR(255) NOT NULL,
                      stationTo VARCHAR(255) NOT NULL,
                      date VARCHAR(255) NOT NULL,
                      vehicle VARCHAR(255) NOT NULL,
                      occupancy VARCHAR(255) NOT NULL,
                      userAgent VARCHAR(255) NOT NULL
                    );

                    DROP TABLE IF EXISTS distance CASCADE;
                    CREATE TABLE distance(
                      stationFrom VARCHAR(255) NOT NULL,
                      stationTo VARCHAR(255) NOT NULL,
                      trip VARCHAR(255) NOT NULL,
                      date VARCHAR(255) NOT NULL,
                      distance INT NOT NULL
                    );

                    DROP VIEW IF EXISTS events;
                    CREATE VIEW events AS (
                      SELECT
                        c.*,
                        c.departuretime AT TIME ZONE 'CET'                    AS departuretime_local,
                        c.arrivaltime AT TIME ZONE 'CET'                      AS arrivaltime_local,
                        sa.name                                               AS arrival,
                        sa.longitude                                          AS arrival_longitude,
                        sa.latitude                                           AS arrival_latitude,
                        sa.country                                            AS arrival_country,
                        sa.longitude :: TEXT                                  AS arrival_longitude_str,
                        sa.latitude :: TEXT                                   AS arrival_latitude_str,
                        EXTRACT(MONTH FROM c.departuretime)                   AS departure_month,
                        EXTRACT(DAY FROM c.departuretime)                     AS departure_day,
                        EXTRACT(DOW FROM c.departuretime)                     AS departure_dow,
                        EXTRACT(HOUR FROM c.departuretime AT TIME ZONE 'CET') AS departure_hour,
                        sd.name                                               AS departure,
                        sd.longitude                                          AS departure_longitude,
                        sd.latitude                                           AS departure_latitude,
                        sd.country                                            AS departure_country,
                        sd.longitude :: TEXT                                  AS departure_longitude_str,
                        sd.latitude :: TEXT                                   AS departure_latitude_str,
                        regexp_replace(c.route, '[^A-Z]', '', 'g')            AS traintype

                      FROM connection c INNER JOIN station sa ON c.arrivalstop = sa.id
                        INNER JOIN station sd ON c.departurestop = sd.id);
                """
                # query= """
                # DROP TABLE IF EXISTS distance;
                #     CREATE TABLE distance(
                #       stationFrom VARCHAR(255) NOT NULL,
                #       stationTo VARCHAR(255) NOT NULL,
                #       vehicle VARCHAR(255) NOT NULL,
                #       date VARCHAR(255) NOT NULL,
                #       distance INT NOT NULL
                #     );
                # """
                cur.execute(query)
                cur.close()

    def extract_connection_files(self, folder, batch_size, last_departure_time=None):
        json_files = [pos_json for pos_json in os.listdir(folder) if pos_json.endswith('.json')].sort()
        result = []
        last_js = ''
        for js in json_files:
            last_js = js
            ts = datetime.strptime(js.replace(".json", ""), "%Y-%m-%dT%H-%M")
            if last_departure_time is not None and ts < last_departure_time:
                continue
            with open(os.path.join(folder, js)) as json_file:
                data = json.load(json_file)
                for conn in data['@graph']:
                    result.append([
                        conn['departureStop'].replace('http://irail.be/stations/NMBS/', ''),
                        conn['departureTime'],
                        conn['departureTime'][:10].replace('-', ''),
                        conn['departureDelay'] if 'departureDelay' in conn else "0",
                        conn['arrivalStop'].replace('http://irail.be/stations/NMBS/', ''),
                        conn['arrivalTime'],
                        conn['arrivalTime'][:10].replace('-', ''),
                        conn['arrivalDelay'] if 'arrivalDelay' in conn else "0",
                        conn['gtfs:route'].replace('http://irail.be/vehicle/', ''),
                        conn['gtfs:trip'].replace('http://irail.be/trips/', ''),
                        conn['@id'].replace('http://irail.be/connections/', '')])
                if len(result) > batch_size:
                    return_value = result
                    result = []
                    yield (js, return_value)
        yield (last_js, result)

    def write_to_db(self, result, table, primary_key=None):
        if len(result) == 0:
            return
        result_string = ['(' + ','.join(['\'%s\'' % c for c in r]) + ')' for r in result]
        result_string = ',\n'.join(result_string)

        query = """
            INSERT INTO %s VALUES
              %s
        """ % (table, result_string)
        if primary_key is not None:
            query += "\nON CONFLICT (%s) DO NOTHING\n" % primary_key

        with connect(self.connection_string) as con:
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                cur.execute(query)
                cur.close()

    def extract_station_file(self, folder):
        with open(os.path.join(folder, 'stations.json')) as json_file:
            result = []
            data = json.load(json_file)
            for station in data['@graph']:
                result.append([
                    station['name'].replace('\'', ' '),
                    station['country'].replace('http://sws.geonames.org/', '').replace('/', ''),
                    station['longitude'],
                    station['latitude'],
                    station['@id'].replace('http://irail.be/stations/NMBS/', '')])
            return result

    def extract_feedback_file(self, folder):
        with open(os.path.join(folder, 'feedback.ndjson')) as f:
            content = f.readlines()
            result = []
            for row in content:
                data = json.loads(row)

                result.append([
                    data['querytime'],
                    data['post']['connection'].replace('http://irail.be/connections/', '').lstrip('0'),
                    data['post']['from'].replace('http://irail.be/stations/NMBS/', ''),
                    data['post']['to'].replace('http://irail.be/stations/NMBS/', '') if 'to' in data['post'] else '',
                    data['post']['date'],
                    data['post']['vehicle'].replace('http://irail.be/vehicle/', ''),
                    data['post']['occupancy'].replace('http://api.irail.be/terms/', ''),
                    data['user_agent']])
            return result

    def extract_distances(self):
        query = """
          SELECT o.stationfrom, o.vehicle, o.date, COUNT(*) AS freq
          FROM occupancy o
          INNER JOIN connection c ON o.stationfrom = c.departurestop
                                 AND o.date = c.departuredate
                                 AND o.vehicle = c.route
          WHERE c.trip <> ''
          GROUP BY o.stationfrom, o.vehicle, o.date
          ORDER by freq
        """
        with connect(self.connection_string) as con:
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with con.cursor() as cur:
                cur.execute(query)
                for row in cur:
                    stationfrom = row[0]
                    vehicle = row[1]
                    date = row[2]
                    print row
                    distances_to_station = self.get_distances_to_station(con, date, stationfrom, vehicle)
                    yield distances_to_station
                cur.close()

    def get_distances_to_station(self, con, date, stationfrom, route):
        query = """
              SELECT departurestop FROM connection c
              WHERE c.route='%s' AND c.departuredate='%s' ORDER BY departuretime
              """ % (route, date)
        with con.cursor() as cur2:
            cur2.execute(query)
            stations = [x[0] for x in cur2.fetchall()]
        result = [[stationfrom, s, route, date, stations.index(s) - stations.index(stationfrom)] for s in stations]
        return result

    def remove_duplicate_connections(self):
        query = """
            DELETE FROM connection
            WHERE exists(SELECT 1
              FROM connection t2
                 WHERE t2.arrivaltime = connection.arrivaltime AND
                       t2.arrivalstop = connection.arrivalstop AND
                       t2.departuretime = connection.departuretime AND
                       t2.departurestop = connection.departurestop AND
                       t2.ctid > connection.ctid);
        """

        with connect(self.connection_string) as con:
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                cur.execute(query)
                cur.close()

    def get_last_departure_time(self):
        query = """ SELECT MAX(departuretime) FROM connection """

        with connect(self.connection_string) as con:
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                if result is not None:
                    last_departure_time = result[0]
                    return last_departure_time
                return None

    def tables_exist(self):
        query = """
            SELECT EXISTS (
               SELECT 1
               FROM   information_schema.tables
               WHERE  table_schema = 'public'
               AND    table_name = 'connection'
            );
        """

        with connect(self.connection_string) as con:
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                if result is not None:
                    exists = result[0]
                    return exists
                return False

    def run(self, batch_size=2000):
        if not self.tables_exist():
            self.wipe = True

        if self.wipe:
            self.create_tables()
            results = self.extract_station_file(path.join(self.folder, 'stations'))
            self.write_to_db(results, table='station')
            results = self.extract_feedback_file(path.join(self.folder, 'feedback'))
            self.write_to_db(results, table='occupancy')

        last_departure_time = self.get_last_departure_time()
        results = self.extract_connection_files(folder=path.join(self.folder, 'connections'),
                                                batch_size=batch_size,
                                                last_departure_time=last_departure_time)
        for result in results:
            self.write_to_db(result[1], table='connection', primary_key='id')
            print '%s processed' % result[0]

        if self.wipe:
            results = self.extract_distances()
            for result in results:
                self.write_to_db(result, table='distance')
