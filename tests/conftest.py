# -*- coding: utf-8 -*-
from logging import getLogger
from peewee import DateField, CharField

import psycopg2
import pymysql
import pytest

logger = getLogger(__name__)


@pytest.mark.no_cover
def check_mysql(mysql_conf) -> bool:
    try:
        connection = pymysql.connect(**mysql_conf)
        return connection.open
    except pymysql.OperationalError:  # pragma: no cover
        logger.info("Waitting for MySQL starting completed.")
        return False


@pytest.fixture(scope="session")
def mysql(docker_ip, docker_services):
    port = docker_services.port_for("mysql", 3306)
    mysql_conf = {
        "host": docker_ip,
        "port": port,
        "user": "ruiamysql",
        "password": "abc123",
        "database": "ruiamysql",
    }
    docker_services.wait_until_responsive(lambda: check_mysql(mysql_conf), 300, 10)
    return mysql_conf


@pytest.fixture(scope="function")
def mysql_config():
    return {
        "host": "somehost",
        "port": 1234,
        "user": "ruiamysql",
        "password": "abc123",
        "database": "ruiamysql",
        "model": {
            "table_name": "test",
            "some_date": DateField(),
            "some_char": CharField(),
        },
    }


@pytest.fixture(scope="function")
def pool_mysql_config():
    return {
        "host": "somehost",
        "port": 1234,
        "user": "ruiamysql",
        "password": "abc123",
        "database": "ruiamysql",
        "pool": True,
        "min_connections": 5,
        "max_connections": 20,
        "model": {
            "table_name": "test",
            "some_date": DateField(),
            "some_char": CharField(),
        },
    }


@pytest.fixture(scope="function")
def postgres_config():
    return {
        "host": "somehost",
        "port": 1234,
        "user": "ruiapostgres",
        "password": "abc123",
        "database": "ruiapostgres",
        "model": {
            "table_name": "test",
            "some_date": DateField(),
            "some_char": CharField(),
        },
    }


@pytest.fixture(scope="function")
def pool_postgres_config():
    return {
        "host": "somehost",
        "port": 1234,
        "user": "ruiamysql",
        "password": "abc123",
        "database": "ruiamysql",
        "pool": True,
        "min_connections": 5,
        "max_connections": 20,
        "model": {
            "table_name": "test",
            "some_date": DateField(),
            "some_char": CharField(),
        },
    }


@pytest.mark.no_cover
def check_postgres(postgres_conf):
    try:
        conn = psycopg2.connect(**postgres_conf)
        return conn.status == psycopg2.extensions.STATUS_READY
    except psycopg2.OperationalError:  # pragma: no cover
        logger.info("Waitting for PostgreSQL starting completed.")
        return False


@pytest.fixture(scope="session")
def postgresql(docker_ip, docker_services):
    port = docker_services.port_for("postgres", 5432)
    postgres_conf = {
        "host": docker_ip,
        "port": port,
        "user": "ruiapostgres",
        "password": "abc123",
        "database": "ruiapostgres",
    }
    docker_services.wait_until_responsive(
        lambda: check_postgres(postgres_conf), 300, 10
    )
    return postgres_conf
