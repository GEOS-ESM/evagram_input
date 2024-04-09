from evagram_input import input_tool
import unittest
import psycopg2


conn = psycopg2.connect("host=127.0.0.1 port=5432 dbname=plots user=postgres")
wf_dictionary = {
    'owner': 'postgres',
    'experiment': 'experiment1',
    'eva_directory': 'tests/eva'
}
session = input_tool.Session(wf_dictionary)
session.input_data()


class TestDatabaseInputTool(unittest.TestCase):
    def setUp(self):
        self.cur = conn.cursor()

        self.cur.execute(
            """SELECT setval('owners_owner_id_seq',
            (SELECT MAX(owner_id) FROM owners)+1)""")
        self.cur.execute(
            """SELECT setval('experiments_experiment_id_seq',
            (SELECT MAX(experiment_id) FROM experiments)+1)""")
        self.cur.execute(
            """SELECT setval('plots_plot_id_seq',
            (SELECT MAX(plot_id) FROM plots)+1)""")
        self.cur.execute(
            """SELECT setval('observations_observation_id_seq',
            (SELECT MAX(observation_id) FROM observations)+1)""")

    def tearDown(self):
        conn.rollback()
        self.cur.close()

    def test_OwnerInSession(self):
        self.cur.execute(
            "SELECT (username) FROM owners WHERE username=%s", ("postgres",))
        assert len(self.cur.fetchall()) == 1

    def test_ExperimentInSession(self):
        self.cur.execute(
            "SELECT (experiment_name) FROM experiments WHERE experiment_name=%s AND owner_id=%s",
            ("experiment1", 1)
        )
        assert len(self.cur.fetchall()) == 1

    def test_WrongRootOwner(self):
        with self.assertRaises(Exception):
            wf_dictionary = {
                'owner': 'test',
                'experiment': 'experiment1',
                'eva_directory': 'tests/eva'
            }
            session = input_tool.Session(wf_dictionary)
            session.input_data()

    def test_ExperimentPathNotFound(self):
        with self.assertRaises(FileNotFoundError):
            wf_dictionary = {
                'owner': 'postgres',
                'experiment': 'experiment1',
                'eva_directory': 'not/a/path'
            }
            session = input_tool.Session(wf_dictionary)
            session.input_data()

    def test_GroupsRelationWithPlotsAndExperiments(self):
        self.cur.execute("""SELECT group_name, plots.plot_id, experiments.experiment_id FROM groups
                            JOIN plots ON plots.group_id = groups.group_id
                            JOIN experiments ON experiments.experiment_id = plots.experiment_id
                            WHERE group_name = %s;""", ("effectiveerror-vs-gsifinalerror",))
        plots, experiments = [], []
        for item in self.cur.fetchall():
            assert len(item) == 3  # sanity checks
            assert item[0] == "effectiveerror-vs-gsifinalerror"
            plots.append(item[1])
            experiments.append(item[2])
        # check that plots associated with group have unique plot ids (relation)
        self.assertEqual(plots, list(set(plots)))
        # check that experiments associated with group can be the same (no relation)
        self.assertGreaterEqual(len(experiments), len(set(experiments)))


unittest.main()
