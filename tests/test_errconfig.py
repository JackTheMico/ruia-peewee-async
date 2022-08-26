# -*- coding: utf-8 -*-

import pytest

from ruia_peewee_async import ParameterError, after_start

from .common import Insert, Update, RuiaPeeweeInsert, RuiaPeeweeUpdate


@pytest.mark.no_cover
@pytest.fixture(scope="class")
def docker_setup():
    return False


@pytest.mark.no_cover
@pytest.fixture(scope="class")
def docker_cleansup():
    return False


class TestErrConfig:
    async def test_targetdb_error(self, event_loop):
        class Temp:
            def __init__(
                self,
                data,
                database,
                query=None,
                create_when_not_exists=True,
                not_update_when_exists=True,
                only=None,
            ):
                self.data = data
                self.database = database
                self.query = query
                self.create_when_not_exists = create_when_not_exists
                self.not_update_when_exists = not_update_when_exists
                self.only = only

        with pytest.raises(ParameterError):
            insert = Insert(loop=event_loop)
            await RuiaPeeweeInsert.process(insert, Temp("testdata", "errorstr"))
        with pytest.raises(ParameterError):
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(
                update, Temp("testdata", "errorstr", query="")
            )

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
