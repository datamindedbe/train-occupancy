import datetime
import unittest

from ingestion.ingest import ingest


class IngestTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        FOLDER = '../data/'
        URL_ROOT = 'http://graph.spitsgids.be/connections/?departureTime='
        STATIONS_URL = 'https://irail.be/stations/NMBS'
        FEEDBACK_URL = 'https://gtfs.irail.be/nmbs/feedback/occupancy-until-20161029.newlinedelimitedjsonobjects'
        START = datetime(2016, 11, 1, 1, 0, 0)
        END = datetime(2016, 12, 4, 0, 0, 0)

        ingest(URL_ROOT, STATIONS_URL, FEEDBACK_URL, START, END, FOLDER)
        ingest()


    @classmethod
    def tearDownClass(self):
        self._postgresql.stop()
        self._postgresql.child_process = None

    def test_all_connection_data_loaded_into_database(self):
        actual_result = self._engine.execute("SELECT * FROM connection").fetchall()
        expected_result = \
            [(u'008812005', datetime.datetime(2016, 10, 30, 21, 40), u'20161030', 0.0, u'008813003',
              datetime.datetime(2016, 10, 30, 21, 38), u'20161030', 0.0, u'IC1544', u'IC154440371',
              u'8812005/20161030/IC1544'),
             (u'008814001', datetime.datetime(2016, 10, 30, 21, 40), u'20161030', 60.0, u'008895802',
              datetime.datetime(2016, 10, 30, 21, 37), u'20161030', 60.0, u'IC2345', u'IC234552141',
              u'8814001/20161030/IC2345'),
             (u'008845146', datetime.datetime(2016, 10, 30, 21, 50), u'20161030', 60.0, u'008845203',
              datetime.datetime(2016, 10, 30, 21, 49), u'20161030', 0.0, u'IC123', u'IC1232501',
              u'8845146/20161030/IC123'),
             (u'008831807', datetime.datetime(2016, 10, 30, 21, 50), u'20161030', 300.0, u'008833605',
              datetime.datetime(2016, 10, 30, 21, 49), u'20161030', 300.0, u'IC1545', u'IC154552031',
              u'8831807/20161030/IC1545')]
        self.assertEquals(len(expected_result), len(actual_result), "Not all connection data loaded in database")
        self.assertListEqual(expected_result, actual_result, "Connection Data not stored correctly in database")

    def test_all_station_data_loaded_into_database(self):
        actual_result = self._engine.execute("SELECT * FROM station").fetchall()
        expected_result = \
            [(u'London Saint Pancras International', u'2635167', -0.1260606, 51.5310399, u'007015400'),
             (u'Heusden', u'2802361', 5.281782, 51.038207, u'008832243'),
             (u'Zolder', u'2802361', 5.3299, 51.033548, u'008832250'),
             (u'Zonhoven', u'2802361', 5.348815, 50.989557, u'008832334'),
             (u'Kiewit', u'2802361', 5.350226, 50.954841, u'008832375'),
             (u'Mol', u'2802361', 5.116336, 51.19105, u'008832409')]
        self.assertEquals(len(expected_result), len(actual_result), "Not all station data loaded in database")
        self.assertListEqual(expected_result, actual_result, "Station Data not stored correctly in database")

    def test_all_occupancy_data_loaded_into_database(self):
        actual_result = self._engine.execute("SELECT * FROM occupancy").fetchall()
        expected_result = \
            [(datetime.datetime(2016, 9, 25, 14, 13, 14), u'7015400/20160925/EUR9108', u'007015400', u'007015440',
              u'20160925', u'EUR9108', u'medium', u'Railer/1610 CFNetwork/808.0.2 Darwin/16.0.0'), (
                 datetime.datetime(2016, 10, 10, 20, 36, 22), u'8821121/20161010/IC4528', u'008821121', u'008813003',
                 u'20161010', u'IC4528', u'high', u'Railer/1610 CFNetwork/808.0.2 Darwin/16.0.0')]
        self.assertEquals(len(expected_result), len(actual_result), "Not all occupancy data loaded in database")
        self.assertListEqual(expected_result, actual_result, "Occupancy Data not stored correctly in database")
