# -*- coding: utf-8 -*-
# import pymysql
import pytest

# def check(mysql_config):
# connection = pymysql.connect(**mysql_config)
# pass


@pytest.fixture(scope="session")
def mysql(docker_ip, docker_services):
    port = docker_services.port_for("mysql", 3306)
    return {"host": docker_ip, "port": port}
