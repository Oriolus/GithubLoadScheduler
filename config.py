import yaml


class Config(object):
    class DbSettings(object):
        def __init__(self, host, database, user, password):
            self.host = host
            self.database = database
            self.user = user
            self.password = password

    def __init__(self):
        self.Db = None  # type: Config.DbSettings
        self.db_min_connections = None  # type: int
        self.db_max_connections = None  # type: int

        self.gh_per_page = None  # type: int

        self.sched_object_per_token = None  # type: int
        self.sched_queue_threshold = None  # type: int
        self.sched_mark_timestamp_delta = None  # type: int
        self.sched_db = None  # type: Config.DbSettings


def get_config(file_name: str = 'config.yaml', encoding: str = 'utf-8') -> Config:
    conf = Config()
    y_conf = None
    with open(file_name, 'r', encoding=encoding) as f_conf:
        y_conf = yaml.safe_load(f_conf)
    if y_conf:
        conf.Db = Config.DbSettings(
            y_conf['db_settings']['host'],
            y_conf['db_settings']['database'],
            y_conf['db_settings']['user'],
            y_conf['db_settings']['password']
        )
        conf.db_min_connections = y_conf['db_settings']['min_connections']
        conf.db_max_connections = y_conf['db_settings']['max_connections']

        conf.gh_per_page = y_conf['github_api']['per_page']

        conf.sched_mark_timestamp_delta = y_conf['scheduler']['sched_mark_timestamp_delta']
        conf.sched_queue_threshold = y_conf['scheduler']['sched_queue_threshold']
        conf.sched_object_per_token = y_conf['scheduler']['sched_object_per_token']
        conf.sched_db = Config.DbSettings(
            y_conf['scheduler']['db_host'],
            y_conf['scheduler']['db_database'],
            y_conf['scheduler']['db_user'],
            y_conf['scheduler']['db_password']
        )

    return conf
