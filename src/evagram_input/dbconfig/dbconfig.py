from . import config
from . import test_config


class DatabaseConfiguration:
    """The database configuration object configures keyword arguments for the evagram database
    connection based on the current stage of the application development life cycle."""
    def __init__(self, test_local=False):
        self.username = config.user
        self.dbname = config.dbname
        self.host = config.host

        if test_local:
            self.username = test_config.user
            self.dbname = test_config.dbname
            self.host = test_config.host

    def get_db_parameters(self):
        arguments = {
            "dbname": self.dbname,
            "user": self.username,
            "host": self.host
        }
        return arguments
