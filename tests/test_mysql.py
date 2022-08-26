# -*- coding: utf-8 -*-
import asyncio
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import after_start, create_model

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
    mysql.update(
        {
            "model": {
                "table_name": "ruia_mysql",
                "title": CharField(),
                "url": CharField(),
            }
        }
    )
    return mysql


class TestMySQL:
    @pytest.mark.dependency()
    async def test_mysql_insert(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        spider_ins = await MySQLInsert.async_start(
            loop=event_loop, after_start=after_start(mysql=mysql)
        )
        count = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        assert count >= 10, "Should insert 10 rows in MySQL."

    @pytest.mark.dependency(depends=["TestMySQL::test_mysql_insert"])
    async def test_mysql_update(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop, after_start=after_start(mysql=mysql)
        )
        one = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, id=randint(1, 11)
        )
        assert one.url == "http://testing.com"

    async def test_mysql_update_does_not_exist(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        mysql["model"]["table_name"] = "ruia_mysql_notexist"
        model, _ = create_model(create_table=True, mysql=mysql)
        rows_before = model.select().count()
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(5)
        rows_after = await spider_ins.mysql_manager.count(
            spider_ins.mysql_model.select()
        )
        assert rows_before <= 3
        assert rows_after > 0
