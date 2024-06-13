from evagram_input.dbconfig import dbconfig
import pickle
import os
import psycopg2


class Session(object):
    """The session object opens a connection to the evagram database and serves as the
    central point for inputting data from a swell task."""
    def __init__(self, owner: str, experiment: str, eva_directory: str):
        self.owner = owner
        self.experiment = experiment
        self.eva_directory = eva_directory
        self.owner_id = None
        self.experiment_id = None

        self._conn = None
        self._cursor = None
        self._num_diagnostics = 0
        self._status_message = "CONNECTION OPEN"

        # Warning: Set test_local to False for production
        self._dbconfig = dbconfig.DatabaseConfiguration(test_local=True)
        self._dbparams = self._dbconfig.get_db_parameters()

    def __repr__(self):
        message = f"Status: {self._conn.closed} ({self._status_message})\n"
        message += f"Owner: {self.owner}\n"
        message += f"Experiment: {self.experiment}\n"
        message += f"Number of diagnostics added: {self._num_diagnostics}\n"
        return message

    def input_data(self):
        try:
            self._conn = psycopg2.connect(**self._dbparams)
            self._cursor = self._conn.cursor()
            self._verify_session_user()
            self.owner_id = self._add_current_user(self.owner)
            self.experiment_id = self._add_current_experiment(self.experiment, self.owner_id)
            self._run_task()
        except psycopg2.OperationalError:
            self._status_message = "CONNECTION CLOSED AND ABORTED"
            self._num_diagnostics = 0
            raise
        except FileNotFoundError:
            self._status_message = "CONNECTION CLOSED AND ABORTED"
            self._num_diagnostics = 0
            raise
        except RuntimeError:
            self._status_message = "CONNECTION CLOSED AND ABORTED"
            self._num_diagnostics = 0
            raise
        except RuntimeWarning:
            self._status_message = "CONNECTION CLOSED WITH WARNING"
            self._num_diagnostics = 0
            raise
        else:
            self._status_message = "CONNECTION CLOSED WITH SUCCESS"
            self._conn.commit()
        finally:
            print("Terminating current workflow session...")
            self._cursor.close()
            self._conn.close()
            print("Session object closed!")
            print(self)

    def _run_task(self):
        eva_directory_path = self.eva_directory
        files_found = 0
        for observation_dir in os.listdir(eva_directory_path):
            observation_path = os.path.join(eva_directory_path, observation_dir)
            if os.path.isdir(observation_path):
                for plot in os.listdir(observation_path):
                    if plot.endswith(".pkl"):
                        self._add_plot(
                            eva_directory_path, observation_dir, plot, self.experiment_id)
                        files_found += 1

        if files_found == 0:
            raise RuntimeWarning(("There was no diagnostics found in the given directory. "
                                  "Make sure parameter 'eva_directory' contains "
                                  "expected directory structure."))

    def _verify_session_user(self):
        self._cursor.execute("SELECT pg_backend_pid();")
        conn_pid = self._cursor.fetchone()[0]
        self._cursor.execute("SELECT usename FROM pg_stat_activity WHERE pid=%s", (conn_pid,))
        conn_username = self._cursor.fetchone()[0]
        if conn_username != self.owner:
            raise RuntimeError((f"Connection refused, workflow owner '{self.owner}' does not match "
                                "with username in database instance."))

    def _insert_table_record(self, data, table):
        self._cursor.execute(f"SELECT * FROM {table} LIMIT 0")
        colnames = [desc[0] for desc in self._cursor.description]
        # filter data to contain only existing columns in table
        data = {k: v for (k, v) in data.items() if k in colnames}

        # check if record exists in table
        query = f"SELECT * FROM {table} WHERE "
        and_clause = [f"{key}=%s" for key in data]
        and_clause_str = " AND ".join(and_clause)
        query += and_clause_str
        self._cursor.execute(query, tuple(data.values()))

        if len(self._cursor.fetchall()) == 0:
            query = f"INSERT INTO {table} ("
            query += ', '.join(data)
            query += ") VALUES ("
            query += ', '.join(["%s" for _ in range(len(data))])
            query += ")"
            self._cursor.execute(query, tuple(data.values()))
            if table == "plots":
                self._num_diagnostics += 1

    def _add_current_user(self, username):
        # Adds the current user in the workflow and returns its identifier in the database
        self._cursor.execute("SELECT (owner_id) FROM owners WHERE username=%s", (username,))
        current_user = self._cursor.fetchall()
        if len(current_user) == 1:
            # returns owner_id of current user
            return current_user[0][0]
        else:
            # creates a new user by the specified username
            user_obj = {"username": username}
            self._insert_table_record(user_obj, "owners")
            return self._add_current_user(username)

    def _add_current_experiment(self, experiment_name, owner_id):
        # Adds the current experiments conducted by user in the workflow
        # and returns its identifier in the database
        self._cursor.execute("""SELECT (experiment_id) FROM experiments
                    WHERE experiment_name=%s AND owner_id=%s""", (experiment_name, owner_id))
        current_experiment = self._cursor.fetchall()
        if len(current_experiment) == 1:
            # returns experiment_id of current experiment
            return current_experiment[0][0]
        else:
            # creates a new experiment by the specified experiment name
            experiment_obj = {
                "experiment_name": experiment_name,
                "owner_id": owner_id
            }
            self._insert_table_record(experiment_obj, "experiments")
            return self._add_current_experiment(experiment_name, owner_id)

    def _add_plot(self, experiment_path, observation_name, plot_filename, experiment_id):
        plot_file_path = os.path.join(
            experiment_path, observation_name, plot_filename)

        with open(plot_file_path, 'rb') as file:
            try:
                dictionary = pickle.load(file)
            except Exception:
                raise RuntimeError(("There was a problem loading the diagnostics file "
                                    f"'{plot_filename}' in the given directory. Please try again."))

        # extract the div and script components
        div = dictionary['div']
        script = dictionary['script']

        # parse filename for components variable name, channel, and group name
        filename_no_extension = os.path.splitext(plot_filename)[0]
        plot_components = filename_no_extension.split("_")

        try:
            assert (len(plot_components) == 3)
            var_name = plot_components[0]
            channel = plot_components[1] if plot_components[1] != '' else None
            group_name = plot_components[2]
        except Exception:
            raise RuntimeError((f"Could not properly parse the filename '{plot_filename}' "
                                "in the given directory. Please try again."))

        # insert observation, variable, group dynamically if not exist in database
        self._cursor.execute("SELECT observation_id FROM observations WHERE observation_name=%s",
                             (observation_name,))
        new_observation = len(self._cursor.fetchall()) == 0
        self._cursor.execute(
            """SELECT variable_id FROM variables WHERE variable_name=%s
            AND (channel=%s OR channel IS NULL)""",
            (var_name, channel))
        new_variable = len(self._cursor.fetchall()) == 0
        self._cursor.execute("SELECT group_id FROM groups WHERE group_name=%s", (group_name,))
        new_group = len(self._cursor.fetchall()) == 0

        if new_observation:
            observation_obj = {
                "observation_name": observation_name,
            }
            self._insert_table_record(observation_obj, "observations")

        if new_variable:
            variable_obj = {
                "variable_name": var_name,
                "channel": channel
            }
            self._insert_table_record(variable_obj, "variables")

        if new_group:
            group_obj = {
                "group_name": group_name
            }
            self._insert_table_record(group_obj, "groups")

        # get the observation, variable, group ids
        self._cursor.execute("SELECT observation_id FROM observations WHERE observation_name=%s",
                             (observation_name,))
        observation_id = self._cursor.fetchone()[0]
        self._cursor.execute(
            """SELECT variable_id FROM variables WHERE variable_name=%s
            AND (channel=%s OR channel IS NULL)""",
            (var_name, channel))
        variable_id = self._cursor.fetchone()[0]
        self._cursor.execute("SELECT group_id FROM groups WHERE group_name=%s", (group_name,))
        group_id = self._cursor.fetchone()[0]

        # create plot object
        plot_obj = {}
        plot_obj["div"] = div
        plot_obj["script"] = script
        plot_obj["experiment_id"] = experiment_id
        plot_obj["observation_id"] = observation_id
        plot_obj["group_id"] = group_id
        plot_obj["variable_id"] = variable_id

        # insert plot to database
        self._insert_table_record(plot_obj, "plots")
