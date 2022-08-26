# -*- coding: utf-8 -*-

import pytest

from ruia_peewee_async import ParameterError, after_start

from .common import Insert, Update


@pytest.mark.no_cover
@pytest.fixture(scope="class")
def docker_setup():
    return False


@pytest.mark.no_cover
@pytest.fixture(scope="class")
def docker_cleansup():
    return False


class TestErrConfig:
    async def test_noconfig(
        self, docker_setup, docker_cleanup, event_loop
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        with pytest.raises(ParameterError):
            await Insert.async_start(loop=event_loop, after_start=after_start())
        with pytest.raises(ParameterError):
            await Insert.async_start(loop=event_loop, after_start=after_start(mysql={}))
        with pytest.raises(ParameterError):
            await Insert.async_start(
                loop=event_loop, after_start=after_start(postgres={})
            )
        with pytest.raises(ParameterError):
            await Insert.async_start(
                loop=event_loop, after_start=after_start(postgres={}, mysql={})
            )

    async def test_no_model(
        self, docker_setup, docker_cleanup, event_loop
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        mysql = {"host": "somehost"}
        postgres = {"host": "somehost"}
        with pytest.raises(ParameterError):
            await Update.async_start(
                loop=event_loop, after_start=after_start(mysql=mysql)
            )
        with pytest.raises(ParameterError):
            await Update.async_start(
                loop=event_loop, after_start=after_start(postgres=postgres)
            )
