import os
import unittest

import sys
import testing.postgresql
import utils
from ingestion.etl import Etl
from sqlalchemy import create_engine
from sqlalchemy.orm import session


class EtlLargeTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self._postgresql = testing.postgresql.Postgresql()
        connection_string = self._postgresql.url()
        self._engine = create_engine(connection_string)
        self._folder = os.path.join(utils.base_dir(), "tests", "data", "large")
        etl = Etl(self._postgresql.url(), self._folder)
        etl.run(batch_size=200)

    @classmethod
    def tearDownClass(self):
        self._postgresql.stop()
        self._postgresql.child_process = None

    def test_all_connection_data_loaded_into_database(self):
        result = self._engine.execute("SELECT COUNT(*) FROM connection")
        count = result.fetchone()[0]
        self.assertEquals(772, count, "Not all connection data loaded in database")

    def test_all_station_data_loaded_into_database(self):
        result = self._engine.execute("SELECT COUNT(*) FROM station")
        count = result.fetchone()[0]
        self.assertEquals(644, count, "Not all station data loaded in database")

    def test_all_occupancy_data_loaded_into_database(self):
        result = self._engine.execute("SELECT COUNT(*) FROM occupancy")
        count = result.fetchone()[0]
        self.assertEquals(2250, count, "Not all occupancy data loaded in database")
