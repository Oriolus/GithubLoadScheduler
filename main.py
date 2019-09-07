import logging
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool

from logging.handlers import RotatingFileHandler

from config import get_config


logger = None
_config = get_config()

pool = ThreadedConnectionPool(
    _config.db_min_connections,
    _config.db_max_connections,
    "dbname='{}' user='{}' host='{}' password='{}'".format(
        _config.Db.database,
        _config.Db.user,
        _config.Db.host,
        _config.Db.password
    )
)


@contextmanager
def transaction(name="transaction", **kwargs):
    options = {
        "isolation_level": kwargs.get("isolation_level", None),
        "readonly": kwargs.get("readonly", None),
        "deferrable": kwargs.get("deferrable", None),
    }

    conn = None
    try:
        conn = pool.getconn()
        conn.set_session(**options)
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("{} error: {}".format(name, e))
        raise e
    finally:
        conn.reset()
        pool.putconn(conn)


def get_logger(logger_file: str = None):
    global logger
    if logger:
        return logger
    if not logger_file:
        logger_file = 'main.log'
    file_handler = RotatingFileHandler(logger_file, mode='a', maxBytes=2e7)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    _formatter = logging.Formatter('%(asctime)s - %(name)s:%(threadName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(_formatter)
    console_handler.setFormatter(_formatter)

    logger = logging.getLogger('repo_loading')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
