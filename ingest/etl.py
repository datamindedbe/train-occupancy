import json
import os

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

USER = 'krispeeters'
HOST = 'localhost'
PWD = ''
DB = 'trains'
DATA = '../data0810'


def create_database(user, host, password, db):
    with connect(user=user, host=host, password=password) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute('DROP DATABASE IF EXISTS ' + db)
            cur.execute('CREATE DATABASE ' + db)
            cur.close()


def create_tables(user, host, password, db):
    with connect(user=user, host=host, password=password, database=db) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            query = """
                DROP TABLE IF EXISTS connection;
                CREATE TABLE connection(
                  departureStop VARCHAR(255) NOT NULL,
                  departureTime TIMESTAMP NOT NULL,
                  departureDate VARCHAR(255) NOT NULL,
                  arrivalStop VARCHAR(255) NOT NULL,
                  arrivalTime TIMESTAMP NOT NULL,
                  arrivalDate VARCHAR(255) NOT NULL,
                  route VARCHAR(255) NOT NULL,
                  trip VARCHAR(255) NOT NULL,
                  id VARCHAR(255) NOT NULL
                );

                DROP TABLE IF EXISTS station;
                CREATE TABLE station(
                  name VARCHAR(255) NOT NULL,
                  country VARCHAR(255) NOT NULL,
                  longitude FLOAT NOT NULL,
                  latitude FLOAT NOT NULL,
                  id VARCHAR(255) NOT NULL
                );

                DROP TABLE IF EXISTS occupancy;
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

                DROP TABLE IF EXISTS distance;
                CREATE TABLE distance(
                  stationFrom VARCHAR(255) NOT NULL,
                  stationTo VARCHAR(255) NOT NULL,
                  trip VARCHAR(255) NOT NULL,
                  date VARCHAR(255) NOT NULL,
                  distance INT NOT NULL
                );
            """
            query= """
            DROP TABLE IF EXISTS distance;
                CREATE TABLE distance(
                  stationFrom VARCHAR(255) NOT NULL,
                  stationTo VARCHAR(255) NOT NULL,
                  vehicle VARCHAR(255) NOT NULL,
                  date VARCHAR(255) NOT NULL,
                  distance INT NOT NULL
                );
            """
            cur.execute(query)
            cur.close()


def extract_json_files(folder):
    json_files = [pos_json for pos_json in os.listdir(folder) if pos_json.endswith('.json')]
    result = []
    for js in json_files:
        with open(os.path.join(folder, js)) as json_file:
            data = json.load(json_file)
            for conn in data['@graph']:
                result.append([
                    conn['departureStop'].replace('http://irail.be/stations/NMBS/', ''),
                    conn['departureTime'],
                    conn['departureTime'][:10].replace('-', ''),
                    conn['arrivalStop'].replace('http://irail.be/stations/NMBS/', ''),
                    conn['arrivalTime'],
                    conn['arrivalTime'][:10].replace('-', ''),
                    conn['gtfs:route'].replace('http://irail.be/vehicle/', ''),
                    conn['gtfs:trip'].replace('http://irail.be/trips/', ''),
                    conn['@id'].replace('http://irail.be/connections/', '')])
            if len(result) > 20000:
                return_value = result
                result = []
                yield (js, return_value)
    yield (js, result)


def write_to_db(result, user, host, password, db, table):
    result_string = ['(' + ','.join(['\'%s\'' % c for c in r]) + ')' for r in result]
    result_string = ',\n'.join(result_string)

    query = """
        INSERT INTO %s VALUES
          %s
    """ % (table, result_string)

    with connect(user=user, host=host, password=password, database=db) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute(query)
            cur.close()


def extract_station_file(folder):
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


def extract_feedback_file(folder):
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


def extract_distances(user, host, password, db):
    query = """
      SELECT o.stationfrom, o.vehicle, o.date, COUNT(*) AS freq FROM occupancy o
      INNER JOIN connection c ON o.stationfrom = c.departurestop
                             AND o.date = c.departuredate
                             AND o.vehicle = c.route
      WHERE c.trip <> ''
      GROUP BY o.stationfrom, o.vehicle, o.date
      ORDER by freq
    """
    with connect(user=user, host=host, password=password, database=db) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with con.cursor() as cur:
            cur.execute(query)
            for row in cur:
                stationfrom = row[0]
                vehicle = row[1]
                date = row[2]
                print row
                distances_to_station = get_distances_to_station(con, date, stationfrom, vehicle)
                yield distances_to_station
            cur.close()


def get_distances_to_station(con, date, stationfrom, route):
    query = """
          SELECT departurestop FROM connection c
          WHERE c.route='%s' AND c.departuredate='%s' ORDER BY departuretime
          """ % (route, date)
    with con.cursor() as cur2:
        cur2.execute(query)
        stations = [x[0] for x in cur2.fetchall()]
    result = [[stationfrom, s, route, date, stations.index(s) - stations.index(stationfrom)] for s in stations]
    return result


def etl(user, host, password, db, folder):
    create_database(user, host, password, db)
    create_tables(user, host, password, db)
    results = extract_station_file(folder)
    write_to_db(results, user, host, password, db, table='station')
    results = extract_feedback_file(folder)
    write_to_db(results, user, host, password, db, table='occupancy')
    results = extract_json_files(folder)
    for result in results:
        write_to_db(result[1], user, host, password, db, table='connection')
        print '%s processed' % result[0]
    results = extract_distances(user, host, password, db)
    for result in results:
        write_to_db(result, user, host, password, db, table='distance')


etl(USER, HOST, PWD, DB, DATA)
