from psycopg import connect
from psycopg.rows import dict_row

from faersdb.config import settings


def get_conn():
    return connect(settings.pg_dsn)


def get_dict_conn():
    return connect(settings.pg_dsn, row_factory=dict_row)