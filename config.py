import yaml


class Config(object):
    def __init__(self):
        self.db_host = None  # type: str
        self.db_user = None  # type: str
        self.db_password = None  # type: str
        self.db_database = None  # type: str

        self.gh_per_page = None  # type: int

        self.sched_object_per_token = None  # type: int
        self.sched_queue_threshold = None  # type: int
        self.sched_mark_timestamp_delta = None  # type: int


def get_config(file_name: str = 'config.yaml', encoding: str = 'utf-8') -> Config:
    conf = Config()
    y_conf = None
    with open(file_name, 'r', encoding=encoding) as f_conf:
        y_conf = yaml.safe_load(f_conf)
    if y_conf:
        conf.db_host = y_conf['db_settings']['host']
        conf.db_user = y_conf['db_settings']['user']
        conf.db_password = y_conf['db_settings']['password']
        conf.db_database = y_conf['db_settings']['database']

        conf.gh_per_page = y_conf['github_api']['per_page']

        conf.sched_mark_timestamp_delta = y_conf['scheduler']['sched_mark_timestamp_delta']
        conf.sched_queue_threshold = y_conf['scheduler']['sched_queue_threshold']
        conf.sched_object_per_token = y_conf['scheduler']['sched_object_per_token']

    return conf
