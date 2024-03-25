import pickle
import os


class Session:
    def __init__(self, conn, wf_dictionary):
        self.conn = conn
        self.wf_dictionary = wf_dictionary
        self.owner_id = None
        self.experiment_id = None
        self.open_session()

    def open_session(self):
        # Verifying expected parameters
        required = {'owner', 'experiment', 'eva_directory'}
        difference = required.difference(self.wf_dictionary)
        if len(difference) > 0:
            print(f"Missing required parameters for workflow dictionary: {difference}")
            return 1
        with self.conn:
            with self.conn.cursor() as cur:
                self.owner_id = add_current_user(cur, self.wf_dictionary["owner"])
                self.experiment_id = add_current_experiment(
                    cur, self.wf_dictionary["experiment"], self.owner_id)
                print("Session created successfully! Running task...")
                self.run_task()
                print("Task completed!")
        return 0

    def run_task(self):
        eva_directory_path = self.wf_dictionary["eva_directory"]
        with self.conn.cursor() as cur:
            for observation_dir in os.listdir(eva_directory_path):
                observation_path = os.path.join(eva_directory_path, observation_dir)
                if os.path.isdir(observation_path):
                    for plot in os.listdir(observation_path):
                        if plot.endswith(".pkl"):
                            add_plot(
                                cur, eva_directory_path, observation_dir, plot, self.experiment_id)
        return 0


def insert_table_record(cur, data, table):
    cur.execute(f"SELECT * FROM {table} LIMIT 0")
    colnames = [desc[0] for desc in cur.description]
    # filter data to contain only existing columns in table
    data = {k: v for (k, v) in data.items() if k in colnames}

    query = f"INSERT INTO {table} ("
    query += ', '.join(data)
    query += ") VALUES ("
    query += ', '.join(["%s" for _ in range(len(data))])
    query += ")"

    cur.execute(query, tuple(data.values()))


def add_current_user(cur, username):
    # Adds the current user in the workflow and returns its identifier in the database
    # TODO: perform validation checks on arguments
    cur.execute("SELECT (owner_id) FROM owners WHERE username=%s", (username,))
    current_user = cur.fetchall()
    if len(current_user) == 1:
        # returns owner_id of current user
        return current_user[0][0]
    else:
        # creates a new user by the specified username
        user_obj = {"username": username}
        insert_table_record(cur, user_obj, "owners")
        return add_current_user(cur, username)


def add_current_experiment(cur, experiment_name, owner_id):
    # Adds the current experiments conducted by user in the workflow
    # and returns its identifier in the database
    cur.execute("""SELECT (experiment_id) FROM experiments
                   WHERE experiment_name=%s AND owner_id=%s""", (experiment_name, owner_id))
    current_experiment = cur.fetchall()
    if len(current_experiment) == 1:
        # returns experiment_id of current experiment
        return current_experiment[0][0]
    else:
        # creates a new experiment by the specified experiment name
        experiment_obj = {
            "experiment_name": experiment_name,
            "owner_id": owner_id
        }
        insert_table_record(cur, experiment_obj, "experiments")
        return add_current_experiment(cur, experiment_name, owner_id)


def add_plot(cur, experiment_path, observation_name, plot_filename, experiment_id):
    plot_file_path = os.path.join(
        experiment_path, observation_name, plot_filename)

    with open(plot_file_path, 'rb') as file:
        dictionary = pickle.load(file)

    # extract the div and script components
    div = dictionary['div']
    script = dictionary['script']

    # parse filename for components variable name, channel, and group name
    filename_no_extension = os.path.splitext(plot_filename)[0]
    plot_components = filename_no_extension.split("_")

    var_name = plot_components[0]
    channel = plot_components[1] if plot_components[1] != '' else None
    group_name = plot_components[2]

    # insert observation, variable, group dynamically if not exist in database
    cur.execute("SELECT observation_id FROM observations WHERE observation_name=%s",
                (observation_name,))
    new_observation = len(cur.fetchall()) == 0
    cur.execute(
        """SELECT variable_id FROM variables WHERE variable_name=%s
        AND (channel=%s OR channel IS NULL)""",
        (var_name, channel))
    new_variable = len(cur.fetchall()) == 0
    cur.execute("SELECT group_id FROM groups WHERE group_name=%s", (group_name,))
    new_group = len(cur.fetchall()) == 0

    if new_observation:
        observation_obj = {
            "observation_name": observation_name,
        }
        insert_table_record(cur, observation_obj, "observations")

    if new_variable:
        variable_obj = {
            "variable_name": var_name,
            "channel": channel
        }
        insert_table_record(cur, variable_obj, "variables")

    if new_group:
        group_obj = {
            "group_name": group_name
        }
        insert_table_record(cur, group_obj, "groups")

    # get the observation, variable, group ids
    cur.execute("SELECT observation_id FROM observations WHERE observation_name=%s",
                (observation_name,))
    observation_id = cur.fetchone()[0]
    cur.execute(
        """SELECT variable_id FROM variables WHERE variable_name=%s
        AND (channel=%s OR channel IS NULL)""",
        (var_name, channel))
    variable_id = cur.fetchone()[0]
    cur.execute("SELECT group_id FROM groups WHERE group_name=%s", (group_name,))
    group_id = cur.fetchone()[0]

    # create plot object
    plot_obj = {}
    plot_obj["div"] = div
    plot_obj["script"] = script
    plot_obj["experiment_id"] = experiment_id
    plot_obj["observation_id"] = observation_id
    plot_obj["group_id"] = group_id
    plot_obj["variable_id"] = variable_id

    # insert plot to database
    insert_table_record(cur, plot_obj, "plots")

    return 0
