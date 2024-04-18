from evagram_input import input_data
import unittest
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


conn = psycopg2.connect("host=postgres port=5432 dbname=plots user=postgres", 
                        password=os.getenv('DB_PASSWORD'))
input_data(owner='postgres', experiment='experiment1', eva_directory='tests/eva')


class TestDatabaseInputTool(unittest.TestCase):
    def setUp(self):
        self.cur = conn.cursor()

    def tearDown(self):
        conn.rollback()
        self.cur.close()

    def test_OwnerInSession(self):
        self.cur.execute(
            "SELECT owner_id FROM owners WHERE username=%s", ("postgres",))
        self.assertEqual(1, len(self.cur.fetchall()))

    def test_ExperimentInSession(self):
        self.cur.execute(
            "SELECT (owner_id) FROM owners WHERE username=%s", ("postgres",))
        queryset = self.cur.fetchone()
        assert len(queryset) == 1
        owner_id = queryset[0]
        self.cur.execute(
            "SELECT (experiment_name) FROM experiments WHERE experiment_name=%s AND owner_id=%s",
            ("experiment1", owner_id)
        )
        assert len(self.cur.fetchall()) == 1

    def test_WrongRootOwner(self):
        with self.assertRaises(Exception):
            input_data(owner='test', experiment='experiment1', eva_directory='tests/eva')

    def test_ExperimentPathNotFound(self):
        with self.assertRaises(FileNotFoundError):
            input_data(owner='postgres', experiment='experiment1', eva_directory='not/a/path')

    def test_RollbackOnException(self):
        with self.assertRaises(Exception):
            input_data(
                owner='postgres', experiment='bad_experiment', eva_directory='tests/dummy')

        self.cur.execute(
            "SELECT (owner_id) FROM owners WHERE username=%s", ("postgres",))
        queryset = self.cur.fetchone()
        assert len(queryset) == 1
        owner_id = queryset[0]
        self.cur.execute(
            "SELECT (experiment_id) FROM experiments WHERE experiment_name=%s AND owner_id=%s",
            ("bad_experiment", owner_id)
        )
        self.assertEqual(0, len(self.cur.fetchall()))
        self.cur.execute(
            "SELECT (observation_id) FROM observations WHERE observation_name=%s",
            ("airs_aqua",)
        )
        self.assertEqual(0, len(self.cur.fetchall()))
        self.cur.execute(
            "SELECT (observation_id) FROM observations WHERE observation_name=%s",
            ("eva",)
        )
        self.assertEqual(0, len(self.cur.fetchall()))


unittest.main()
