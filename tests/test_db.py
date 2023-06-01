import pytest
import sqlalchemy
from sqlalchemy.exc import OperationalError
from src.dbschema import db
from utils.types import DbKey
from psycopg2.extensions import STATUS_READY
from psycopg2 import sql
from logging import getLogger
from decouple import config


logger = getLogger(__name__)


"""------------------- Fixtures -------------------"""
@pytest.fixture(scope='module')
def psycopg_connection(request):
    tp = request.param.upper()

    conn = db.get_connection(
        db_name_key=f'{tp}_DB_NAME',
        user_key=f'{tp}_DB_USER',
        pwd_key=f'{tp}_DB_PASS',
        host_key=f'{tp}_DB_HOST'
    )
    yield conn

    conn.close()


@pytest.fixture(scope='module')
def psql_db(psycopg_connection, request):
    conn = psycopg_connection
    db.create_db(DbKey.testDb.value, conn)

    yield

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                'DROP DATABASE IF EXISTS {};'
            ).format(sql.Identifier(config(request.param)))
        )

    logger.info(f'Dropped database {config(request.param)}')


@pytest.fixture(scope='module')
def psql_user(psycopg_connection, request):
    conn = psycopg_connection
    db.create_user(request.param[0], request.param[1], conn)

    yield

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                'DROP USER IF EXISTS {};'
            ).format(
                sql.Identifier(config(request.param[0]))
            )
        )

    logger.info(f'Dropped user {config(request.param[0])}')


@pytest.fixture(scope='module')
def sqla_engine(request):
    user_key, pwd_key, host_key, dbname_key = request.param
    engine = db.get_engine(
        user_key=user_key,
        pwd_key=pwd_key,
        host_key=host_key,
        dbname_key=dbname_key
    )

    yield engine

"""-------------------------------------------------"""

"""-------------------   Tests   -------------------"""
@pytest.mark.parametrize('psycopg_connection', ['reg'], indirect=True)
def test_get_connection(psycopg_connection):
    conn = psycopg_connection
    assert conn.status == STATUS_READY


@pytest.mark.parametrize('psql_db', [DbKey.testDb.value], indirect=True)
@pytest.mark.parametrize('psycopg_connection', ['admin'], indirect=True)
def test_create_db(psycopg_connection, psql_db):
    conn = psycopg_connection

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                'SELECT EXISTS('
                'SELECT datname FROM pg_catalog.pg_database WHERE datname = %s);'
            ),
            (config(DbKey.testDb.value),) # MUST BE A TUPLE
        )
        exists = cur.fetchone()

    assert exists[0] is True


@pytest.mark.parametrize('psycopg_connection', ['admin'], indirect=True)
@pytest.mark.parametrize(
    'psql_user',
    [(DbKey.testUser.value, DbKey.testPwd.value)],
    indirect=True
)
def test_create_user(psycopg_connection, psql_user):
    conn = psycopg_connection

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                'SELECT EXISTS('
                'SELECT usename FROM pg_catalog.pg_user WHERE usename = %s);'
            ),
            (config(DbKey.testUser.value),) # MUST BE A TUPLE
        )

        exists = cur.fetchone()

    assert exists[0] is True