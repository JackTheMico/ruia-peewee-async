# -*- coding: utf-8 -*-
from logging import getLogger

import psycopg2
import pymysql
import pytest

logger = getLogger(__name__)


@pytest.mark.no_cover
def check_mysql(mysql_config) -> bool:
    try:
        connection = pymysql.connect(**mysql_config)
        return connection.open
    except pymysql.OperationalError:
        logger.info("Waitting for MySQL starting completed.")
        return False


@pytest.fixture(scope="class")
def mysql(docker_ip, docker_services):
    port = docker_services.port_for("mysql", 3306)
    mysql_config = {
        "host": docker_ip,
        "port": port,
        "user": "ruiamysql",
        "password": "abc123",
        "database": "ruiamysql",
    }
    docker_services.wait_until_responsive(lambda: check_mysql(mysql_config), 300, 10)
    return mysql_config


@pytest.mark.no_cover
def check_postgres(postgres_config):
    try:
        conn = psycopg2.connect(**postgres_config)
        return conn.status == psycopg2.extensions.STATUS_READY
    except psycopg2.OperationalError:
        logger.info("Waitting for PostgreSQL starting completed.")
        return False


@pytest.fixture(scope="class")
def postgresql(docker_ip, docker_services):
    port = docker_services.port_for("postgres", 5432)
    postgres_config = {
        "host": docker_ip,
        "port": port,
        "user": "ruiapostgres",
        "password": "abc123",
        "database": "ruiapostgres",
    }
    docker_services.wait_until_responsive(
        lambda: check_postgres(postgres_config), 300, 10
    )
    return postgres_config
