# -*- coding: utf-8 -*-
import asyncio
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import TargetDB, after_start, create_model

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
    mysql.update(
        {
            "model": {
                "table_name": "ruia_mysql",
                "title": CharField(),
                "url": CharField(),
            }
        }
    )
    postgresql.update(
        {
            "model": {
                "table_name": "ruia_postgres",
                "title": CharField(),
                "url": CharField(),
            }
        }
    )
    return mysql, postgresql


class TestBoth:
    @pytest.mark.dependency()
    async def test_both_insert(self, mysql, postgresql, event_loop):
        mysql, postgresql = basic_setup(mysql, postgresql)
        spider_ins = await BothInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
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
        mysql, postgresql = basic_setup(mysql, postgresql)
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
        )
        mysql_one = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, id=randint(1, 11)
        )
        postgres_one = await spider_ins.postgres_manager.get(
            spider_ins.postgres_model, id=randint(1, 11)
        )
        assert mysql_one.url == "http://testing.com"
        assert postgres_one.url == "http://testing.com"

    async def test_both_update_does_not_exist(self, mysql, postgresql, event_loop):
        mysql, postgresql = basic_setup(mysql, postgresql)
        mysql["model"]["table_name"] = "ruia_mysql_both_notexist"
        postgresql["model"]["table_name"] = "ruia_postgres_both_notexist"
        mmodel, _ = create_model(create_table=True, mysql=mysql)
        pmodel, _ = create_model(create_table=True, postgres=postgresql)
        mrows_before = mmodel.select().count()
        prows_before = pmodel.select().count()
        assert mrows_before <= 3
        assert prows_before <= 3
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(1)
        mrows_after = 0
        while mrows_after == 0:
            mrows_after = await spider_ins.mysql_manager.count(
                spider_ins.mysql_model.select()
            )
            await asyncio.sleep(1)
        assert mrows_after > 0
        prows_after = 0
        while prows_after == 0:
            prows_after = await spider_ins.postgres_manager.count(
                spider_ins.postgres_model.select()
            )
            await asyncio.sleep(1)
        assert prows_after > 0
