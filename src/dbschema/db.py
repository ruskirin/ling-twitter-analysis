from logging import getLogger

import psycopg2
from decouple import config
from psycopg2 import sql
from psycopg2._psycopg import connection
from psycopg2.errors import DuplicateDatabase, DuplicateObject
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import Engine, create_engine

from utils.types import EnvKey, DbKey
from dbschema import ModelBase
"""Necessary imports for database setup by SQLAlchemy"""
from .tweet import Tweet
from .user import User
from .place import Place
from .metadata import Metadata


logger = getLogger(__name__)


def setup_db(
        admin_dbname_key: EnvKey,
        admin_user_key: EnvKey,
        admin_pwd_key: EnvKey,
        admin_host_key: EnvKey,
        new_dbname_key: EnvKey,
        new_user_key: EnvKey,
        new_pwd_key: EnvKey,
        new_host_key: EnvKey):

    conn = get_connection(
        db_name_key=admin_dbname_key,
        user_key=admin_user_key,
        pwd_key=admin_pwd_key,
        host_key=admin_host_key
    )
    create_db(
        db_name_key=new_dbname_key,
        conn=conn
    )
    create_user(
        new_user_key=new_user_key,
        new_pwd_key=new_pwd_key,
        conn=conn
    )
    conn.close()

    engine = get_engine(
        user_key=new_user_key,
        pwd_key=new_pwd_key,
        dbname_key=new_dbname_key,
        host_key=new_host_key
    )

    ModelBase.metadata.create_all(engine, checkfirst=True)


def get_connection(
        db_name_key: EnvKey,
        user_key: EnvKey,
        pwd_key: EnvKey,
        host_key: EnvKey) -> connection:
    """
    Create and return a new connection object -- generally used for fundamental
      operations (eg. creating a database, users, granting privileges)
    """
    conn = psycopg2.connect(
        dbname=config(db_name_key),
        user=config(user_key),
        password=config(pwd_key),
        host=config(host_key)
    )
    return conn


def create_db(
        db_name_key: EnvKey,
        conn: connection):
    """
    Create a new database using an admin user
    :param db_name_key: env key with the name to give the new database
    :param conn: psycopg2.connection() to a root/admin postgres database
    """
    # required to run 'CREATE DATABASE ...'
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn.cursor() as cur:
            # statement executed in sql.SQL and sql.Identifier are used to avoid sql
            #  injection attacks
            cur.execute(
                sql.SQL('CREATE DATABASE {};').format(
                    sql.Identifier(config(db_name_key))
                )
            )
    except DuplicateDatabase:
        logger.debug(f'Database {config(db_name_key)} already exists!')


def create_user(
        new_user_key: EnvKey,
        new_pwd_key: EnvKey,
        conn: connection):
    """
    Create a new pgsql user using credentials inside env file
    :param new_user_key: key for the new user's username
    :param new_pwd_key: key for the new user's password
    :param conn: psycopg2.connection() to a root/admin postgres database
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    'CREATE USER {} WITH PASSWORD %s;'
                ).format(
                    sql.Identifier(config(new_user_key)),
                ),
                (config(new_pwd_key), )
            )
    except DuplicateObject:
        logger.debug(f'User {config(new_user_key)} already exists!')


def get_engine(
        user_key=DbKey.prodUser.value,
        pwd_key=DbKey.prodPwd.value,
        host_key=DbKey.prodHost.value,
        dbname_key=DbKey.prodDb.value) -> Engine:

    """Get a SQLAlchemy engine"""
    try:
        # See if database connection established
        engine = create_engine(
            f'postgresql+psycopg2://'
            f'{config(user_key)}:{config(pwd_key)}'
            f'@{config(host_key)}/{config(dbname_key)}'
        )
        engine.connect().close()

        return engine
    except Exception as e:
        msg = f'Could not establish database connection, verify credentials ' \
              f'in .env file'
        logger.exception(f'{msg}\n{e.args}')

        raise ValueError(msg) from e


if __name__ == '__main__':
    engine = get_engine()

    setup_db(
        admin_dbname_key=DbKey.adminDb.value,
        admin_user_key=DbKey.adminUser.value,
        admin_pwd_key=DbKey.adminPwd.value,
        admin_host_key=DbKey.adminHost.value,
        new_dbname_key=DbKey.prodDb.value,
        new_user_key=DbKey.prodUser.value,
        new_pwd_key=DbKey.prodPwd.value,
        new_host_key=DbKey.prodHost.value
    )

    print(ModelBase.metadata)