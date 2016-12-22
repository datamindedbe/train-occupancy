from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config import DB, USER, PWD, HOST


def build_features():
    query = """
    DROP VIEW IF EXISTS connection_features;
    CREATE VIEW connection_features AS (
    WITH frequencies AS (SELECT
                           c.departurestop,
                           count(*) AS freq
                         FROM connection c
                           INNER JOIN station s ON c.departurestop = s.id
                         GROUP BY departurestop, s.name),
        trip AS (SELECT
                   d.*,
                   sf.name                            AS stationfrom_name,
                   st.name                            AS stationto_name,
                   f.freq,
                   CASE WHEN d.distance <= 0
                     THEN 0
                   ELSE
                     f.freq :: FLOAT END              AS absolute_freq,
                   CASE WHEN d.distance = 0
                     THEN 0
                   ELSE
                     f.freq :: FLOAT / d.distance END AS weighted_freq,
                   CASE WHEN d.distance <= 0
                     THEN 0
                   ELSE
                     f.freq :: FLOAT / d.distance END AS weighted_positive_freq
                 FROM distance d INNER JOIN frequencies f ON d.stationto = f.departurestop
                   INNER JOIN station sf ON d.stationfrom = sf.id
                   INNER JOIN station st ON d.stationto = st.id
                 ORDER BY d.trip, d.date, d.distance),
        weighted_freqs AS (SELECT
                             stationfrom,
                             trip,
                             date,
                             AVG(distance)               AS distance,
                             SUM(absolute_freq)          AS absolute_freqs,
                             SUM(weighted_freq)          AS weighted_freqs,
                             SUM(weighted_positive_freq) AS weighted_positive_freqs
                           FROM trip
                           GROUP BY stationfrom, trip, date)
    SELECT
      c.*,
      EXTRACT(MONTH FROM c.departuretime)                   AS month,
      EXTRACT(DAY FROM c.departuretime)                     AS day,
      EXTRACT(DOW FROM c.departuretime)                     AS dow,
      EXTRACT(HOUR FROM c.departuretime)                    AS hour,
      EXTRACT(MINUTE FROM c.departuretime) :: INT / 15 * 15 AS quarter,
      d.name                                                AS departurename,
      a.name                                                AS arrivalname,
      regexp_replace(c.route, '[^A-Z]', '', 'g')            AS traintype,
      f.absolute_freqs,
      f.weighted_freqs,
      f.weighted_positive_freqs,
      f.distance,
      CASE WHEN EXTRACT(HOUR FROM c.departuretime) < 12
        THEN f.weighted_freqs
      ELSE
        -f.weighted_freqs END                               AS am_weighted_freqs,
      CASE WHEN EXTRACT(HOUR FROM c.departuretime) < 12
        THEN 1
      ELSE
        0 END                                               AS before_noon,
      CASE WHEN EXTRACT(HOUR FROM c.departuretime) >= 7 AND EXTRACT(HOUR FROM c.departuretime) <= 10
                AND EXTRACT(DOW FROM c.departuretime) > 0 AND EXTRACT(DOW FROM c.departuretime) < 6
        THEN 1
      ELSE
        0 END                                               AS morning_commute,
      CASE WHEN EXTRACT(HOUR FROM c.departuretime) >= 16 AND EXTRACT(HOUR FROM c.departuretime) <= 19
                AND EXTRACT(DOW FROM c.departuretime) > 0 AND EXTRACT(DOW FROM c.departuretime) < 6
        THEN 1
      ELSE
        0 END                                               AS evening_commute,
      CASE WHEN c.departuredelay = 0 THEN 'no_delay'
      ELSE
        CASE WHEN c.departuredelay <=300 THEN 'max_5min_delay'
        ELSE
          'more_than_5min_delay'
        END
      END                                                   AS delay_category

    FROM occupancy o
      INNER JOIN connection c ON o.stationfrom = c.departurestop AND o.date = c.departuredate AND o.vehicle = c.route
      INNER JOIN weighted_freqs f ON o.stationfrom = f.stationfrom AND o.vehicle = f.trip AND o.date = f.date
      LEFT JOIN station d ON c.departurestop = d.id
      LEFT JOIN station a ON c.arrivalstop = a.id
    ORDER BY occupancy, trip, departuretime);
    """
    with connect(user=USER, host=HOST, password=PWD, database=DB) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute(query)
            cur.close()


build_features()
