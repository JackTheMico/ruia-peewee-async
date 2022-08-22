# -*- coding: utf-8 -*-
from logging import getLogger

import pymysql
import pytest

logger = getLogger(__name__)


def check(mysql_config) -> bool:
    try:
        connection = pymysql.connect(**mysql_config)
        return connection.open
    except pymysql.OperationalError:
        logger.info("Waitting for MySQL starting completed.")
        return False


@pytest.fixture(scope="session")
def mysql(docker_ip, docker_services):
    port = docker_services.port_for("mysql", 3306)
    mysql_config = {
        "host": docker_ip,
        "port": port,
        "user": "root",
        "password": "abc123",
        "database": "ruiamysql",
    }
    mysql_info = f"docker_ip: {docker_ip}, port: {port}"
    logger.info(mysql_info)
    docker_services.wait_until_responsive(lambda: check(mysql_config), 300, 10)
    return mysql_config
