# -*- coding: utf-8 -*-
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import TargetDB, init_spider

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
    async def init_after_start(spider_ins):
        spider_ins.postgres_config = postgresql
        spider_ins.postgres_model = {
            "table_name": "ruia_postgres",
            "title": CharField(),
            "url": CharField(),
        }
        init_spider(spider_ins=spider_ins)

    return init_after_start


class TestPostgreSQL:
    @pytest.mark.dependency()
    async def test_postgres_insert(self, postgresql, event_loop):
        after_start = basic_setup(postgresql)
        spider_ins = await PostgresqlInsert.async_start(
            loop=event_loop, after_start=after_start, target_db=TargetDB.POSTGRES
        )
        count = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert count >= 10, "Should insert 10 rows in PostgreSQL."

    @pytest.mark.dependency(depends=["TestPostgreSQL::test_postgres_insert"])
    async def test_postgres_update(self, postgresql, event_loop):
        after_start = basic_setup(postgresql)
        spider_ins = await PostgresqlUpdate.async_start(
            loop=event_loop, after_start=after_start, target_db=TargetDB.POSTGRES
        )
        one = await spider_ins.postgres_manager.get(
            spider_ins.postgres_model, id=randint(1, 11)
        )
        assert one.url == "http://testing.com"
