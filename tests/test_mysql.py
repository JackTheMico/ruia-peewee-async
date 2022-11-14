# -*- coding: utf-8 -*-
import asyncio
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import after_start, create_model, before_stop

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
    async def test_mysql_filters_insert(self, mysql, event_loop, caplog):
        mysql = basic_setup(mysql)
        spider_ins = await MySQLInsert.async_start(
            loop=event_loop, after_start=after_start(mysql=mysql), filters="url"
        )
        assert "was filtered by filters" in caplog.text
        spider_ins = await MySQLInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            filters=["url", "title"],
        )
        assert "was filtered by filters" in caplog.text
        rows = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        assert rows == 10
        spider_ins = await MySQLInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            filters="url",
            yield_origin=False,
        )
        assert "wasn't filtered by filters" in caplog.text
        one = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, url="http://testinginsert.com"
        )
        assert one.url == "http://testinginsert.com"

    @pytest.mark.dependency(depends=["TestMySQL::test_mysql_filters_insert"])
    async def test_mysql_not_update_when_exists(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            not_update_when_exists=True,
        )
        one = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, id=randint(1, 10)
        )
        assert one.url != "http://testing.com"

    @pytest.mark.dependency(depends=["TestMySQL::test_mysql_not_update_when_exists"])
    async def test_mysql_filters(self, mysql, event_loop, caplog):
        mysql = basic_setup(mysql)
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            filters="url",
            not_update_when_exists=True,
        )
        assert "wasn't filtered by filters" in caplog.text
        rows = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        assert rows == 11
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            filters=["url", "title"],
            not_update_when_exists=True,
            yield_origin=True,
        )
        assert "was filtered by filters" in caplog.text
        rows = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        assert rows == 11

    @pytest.mark.dependency(depends=["TestMySQL::test_mysql_not_update_when_exists"])
    async def test_mysql_update(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            not_update_when_exists=False,
        )

        for mid in range(1, 11):
            one = await spider_ins.mysql_manager.get(spider_ins.mysql_model, id=mid)
            assert one.url == "http://testing.com"
        spider_ins.mysql_model.truncate_table()

    @pytest.mark.dependency(depends=["TestMySQL::test_mysql_update"])
    async def test_mysql_dont_create_when_not_exists(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        model, _ = create_model(create_table=True, mysql=mysql)
        rows_before = model.select().count()
        assert rows_before == 0
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            create_when_not_exists=False,
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(1)
        rows_after = await spider_ins.mysql_manager.count(
            spider_ins.mysql_model.select()
        )
        assert rows_after == 0

    @pytest.mark.dependency(
        depends=["TestMySQL::test_mysql_dont_create_when_not_exists"]
    )
    async def test_mysql_create_when_not_exists(self, mysql, event_loop):
        mysql = basic_setup(mysql)
        model, _ = create_model(create_table=True, mysql=mysql)
        rows_before = model.select().count()
        assert rows_before == 0
        spider_ins = await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(1)
        rows_after = 0
        while rows_after == 0:
            rows_after = await spider_ins.mysql_manager.count(
                spider_ins.mysql_model.select()
            )
            await asyncio.sleep(1)
        assert rows_after == 10

    @pytest.mark.dependency(
        depends=["TestMySQL::test_mysql_create_when_not_exists"]
    )
    async def test_mysql_before_stop(self, mysql, event_loop, caplog):
        mysql = basic_setup(mysql)
        await MySQLInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            before_stop=before_stop
        )
        await MySQLUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql),
            before_stop=before_stop
        )
        assert 'RuntimeError' not in caplog.text
        assert 'Exception' not in caplog.text
