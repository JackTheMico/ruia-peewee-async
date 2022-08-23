# -*- coding: utf-8 -*-
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import init_spider

from .common import Insert, Update


class MySQLInsert(Insert):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


class MySQLUpdate(Update):
    async def parse(self, response):
        async for item in super().parse(response):
            yield item


def basic_setup(mysql):
    async def init_after_start(spider_ins):
        spider_ins.mysql_config = mysql
        spider_ins.mysql_model = {
            "table_name": "ruia_mysql",
            "title": CharField(),
            "url": CharField(),
        }
        init_spider(spider_ins=spider_ins)

    return init_after_start


class TestMySQL:
    @pytest.mark.dependency()
    async def test_mysql_insert(self, mysql, event_loop):
        after_start = basic_setup(mysql)
        spider_ins = await MySQLInsert.async_start(
            loop=event_loop, after_start=after_start
        )
        count = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        assert count >= 10, "Should insert 10 rows in MySQL."

    @pytest.mark.dependency(depends=["TestMySQL::test_mysql_insert"])
    async def test_mysql_update(self, mysql, event_loop):
        after_start = basic_setup(mysql)
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop, after_start=after_start
        )
        one = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, id=randint(1, 11)
        )
        assert one.url == "http://testing.com"
