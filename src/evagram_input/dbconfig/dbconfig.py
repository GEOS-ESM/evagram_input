import config
import test_config


class DatabaseConfiguration:
    """The database configuration object configures keyword arguments for the evagram database
    connection based on the current stage of the application development life cycle."""
    def __init__(self, test_local=False):
        self._username = config.user
        self._dbname = config.dbname
        self._host = config.host

        if test_local:
            self._username = test_config.user
            self._dbname = test_config.dbname
            self._host = test_config.host

    def get_db_parameters(self):
        arguments = {
            "dbname": self._dbname,
            "user": self._username,
            "host": self._host
        }
        return arguments
