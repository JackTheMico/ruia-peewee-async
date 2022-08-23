# -*- coding: utf-8 -*-
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import TargetDB, init_spider

from .common import Insert, Update


class BothInsert(Insert):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


class BothUpdate(Update):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


def basic_setup(mysql, postgresql):
    async def init_after_start(spider_ins):
        spider_ins.mysql_config = mysql
        spider_ins.mysql_model = {
            "table_name": "ruia_mysql_both",
            "title": CharField(),
            "url": CharField(),
        }
        spider_ins.postgres_config = postgresql
        spider_ins.postgres_model = {
            "table_name": "ruia_postgres_both",
            "title": CharField(),
            "url": CharField(),
        }
        init_spider(spider_ins=spider_ins)

    return init_after_start


class TestBoth:
    @pytest.mark.dependency()
    async def test_both_insert(self, mysql, postgresql, event_loop):
        after_start = basic_setup(mysql, postgresql)
        spider_ins = await BothInsert.async_start(
            loop=event_loop, after_start=after_start, target_db=TargetDB.BOTH
        )
        count_mysql = await spider_ins.mysql_manager.count(
            spider_ins.mysql_model.select()
        )
        count_postgres = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert count_mysql >= 10, "Should insert 10 rows in MySQL."
        assert count_postgres >= 10, "Should insert 10 rows in PostgreSQL."

    @pytest.mark.dependency(depends=["TestBoth::test_both_insert"])
    async def test_both_update(self, mysql, postgresql, event_loop):
        after_start = basic_setup(mysql, postgresql)
        spider_ins = await BothUpdate.async_start(
            loop=event_loop, after_start=after_start, target_db=TargetDB.BOTH
        )
        mysql_one = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, id=randint(1, 11)
        )
        postgres_one = await spider_ins.postgres_manager.get(
            spider_ins.postgres_model, id=randint(1, 11)
        )
        assert mysql_one.url == "http://testing.com"
        assert postgres_one.url == "http://testing.com"
