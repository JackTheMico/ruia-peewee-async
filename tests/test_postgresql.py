# -*- coding: utf-8 -*-
import asyncio
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import (
    TargetDB,
    after_start,
    create_model,
    ParameterError,
    RuiaPeeweeInsert,
)

from .common import Insert, Update


class PostgresqlInsert(Insert):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


class PostgresqlUpdate(Update):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


def basic_setup(postgresql):
    postgresql.update(
        {
            "model": {
                "table_name": "ruia_postgres",
                "title": CharField(),
                "url": CharField(),
            }
        }
    )
    return postgresql


class TestPostgreSQL:
    async def test_targetdb_error(self, event_loop):
        class Temp:
            def __init__(self, data, database):
                self.data = data
                self.database = database

        with pytest.raises(ParameterError):
            insert = Insert(loop=event_loop)
            await RuiaPeeweeInsert.process(insert, Temp("testdata", "errorstr"))

    @pytest.mark.dependency()
    async def test_postgres_insert(self, postgresql, event_loop):
        postgresql = basic_setup(postgresql)
        spider_ins = await PostgresqlInsert.async_start(
            loop=event_loop,
            after_start=after_start(postgres=postgresql),
            target_db=TargetDB.POSTGRES,
        )
        count = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert count >= 10, "Should insert 10 rows in PostgreSQL."

    @pytest.mark.dependency(depends=["TestPostgreSQL::test_postgres_insert"])
    async def test_postgres_update(self, postgresql, event_loop):
        postgresql = basic_setup(postgresql)
        spider_ins = await PostgresqlUpdate.async_start(
            loop=event_loop,
            after_start=after_start(postgres=postgresql),
            target_db=TargetDB.POSTGRES,
        )
        one = await spider_ins.postgres_manager.get(
            spider_ins.postgres_model, id=randint(1, 11)
        )
        assert one.url == "http://testing.com"

    async def test_postgres_update_does_not_exist(self, postgresql, event_loop):
        postgresql = basic_setup(postgresql)
        postgresql["model"]["table_name"] = "ruia_postgres_notexist"
        model, _ = create_model(create_table=True, postgres=postgresql)
        rows_before = model.select().count()
        spider_ins = await PostgresqlUpdate.async_start(
            loop=event_loop,
            after_start=after_start(postgres=postgresql),
            target_db=TargetDB.POSTGRES,
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(5)
        rows_after = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert rows_before <= 3
        assert rows_after > 0
