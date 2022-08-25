# -*- coding: utf-8 -*-

import pytest

from ruia_peewee_async import ParameterError, after_start

from .common import Insert, Update


class MySQLInsert(Insert):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


class MySQLUpdate(Update):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


@pytest.fixture(scope="class")
def docker_setup():
    return False


@pytest.fixture(scope="class")
def docker_cleansup():
    return False


class TestAfterStart:
    async def test_noconfig(
        self, docker_setup, docker_cleanup, event_loop
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        with pytest.raises(ParameterError):
            await MySQLInsert.async_start(loop=event_loop, after_start=after_start())
            await MySQLInsert.async_start(
                loop=event_loop, after_start=after_start(mysql={})
            )
            await MySQLInsert.async_start(
                loop=event_loop, after_start=after_start(postgres={})
            )

    async def test_no_model(
        self, docker_setup, docker_cleanup, event_loop
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        mysql = {"host": "somehost"}
        postgres = {"host": "somehost"}
        with pytest.raises(ParameterError):
            await MySQLUpdate.async_start(
                loop=event_loop, after_start=after_start(mysql=mysql)
            )
            await MySQLUpdate.async_start(
                loop=event_loop, after_start=after_start(postgres=postgres)
            )
