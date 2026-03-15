from psycopg import connect
from faersdb.config import settings


def get_conn():
    return connect(settings.pg_dsn)