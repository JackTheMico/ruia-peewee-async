# -*- coding: utf-8 -*-
import asyncio
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import TargetDB, after_start, create_model, before_stop

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
                "table_name": "ruia_mysql_both",
                "title": CharField(),
                "url": CharField(),
            }
        }
    )
    postgresql.update(
        {
            "model": {
                "table_name": "ruia_postgres_both",
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
    async def test_both_filters_insert(self, mysql, postgresql, event_loop, caplog):
        mysql, postgresql = basic_setup(mysql, postgresql)
        spider_ins = await BothInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            filters="url",
            target_db=TargetDB.BOTH,
        )
        assert "was filtered by filters" in caplog.text
        spider_ins = await BothInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            filters=["url", "title"],
            target_db=TargetDB.BOTH,
        )
        assert "was filtered by filters" in caplog.text
        mrows = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        prows = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert mrows == 10
        assert prows == 10
        spider_ins = await BothInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            filters="url",
            target_db=TargetDB.BOTH,
            yield_origin=False,
        )
        assert "wasn't filtered by filters" in caplog.text
        mone = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, url="http://testinginsert.com"
        )
        assert mone.url == "http://testinginsert.com"
        pone = await spider_ins.postgres_manager.get(
            spider_ins.postgres_model, url="http://testinginsert.com"
        )
        assert pone.url == "http://testinginsert.com"

    @pytest.mark.dependency(depends=["TestBoth::test_both_filters_insert"])
    async def test_both_not_update_when_exists(self, mysql, postgresql, event_loop):
        mysql, postgresql = basic_setup(mysql, postgresql)
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            not_update_when_exists=True,
            target_db=TargetDB.BOTH,
        )
        pone = await spider_ins.postgres_manager.get(
            spider_ins.postgres_model, id=randint(1, 10)
        )
        mone = await spider_ins.mysql_manager.get(
            spider_ins.mysql_model, id=randint(1, 10)
        )
        assert pone.url != "http://testing.com"
        assert mone.url != "http://testing.com"

    @pytest.mark.dependency(depends=["TestBoth::test_both_not_update_when_exists"])
    async def test_both_filters(self, mysql, postgresql, event_loop, caplog):
        mysql, postgresql = basic_setup(mysql, postgresql)
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            filters="url",
            not_update_when_exists=True,
            target_db=TargetDB.BOTH,
        )
        assert "wasn't filtered by filters" in caplog.text
        mrows = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        prows = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert mrows == 11
        assert prows == 11
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            filters=["url", "title"],
            not_update_when_exists=True,
            yield_origin=True,
            target_db=TargetDB.BOTH,
        )
        assert "was filtered by filters" in caplog.text
        mrows = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
        prows = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert mrows == 11
        assert prows == 11

    @pytest.mark.dependency(depends=["TestBoth::test_both_not_update_when_exists"])
    async def test_both_update(self, mysql, postgresql, event_loop):
        mysql, postgresql = basic_setup(mysql, postgresql)
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
            not_update_when_exists=False,
        )
        for mid in range(1, 11):
            mysql_one = await spider_ins.mysql_manager.get(
                spider_ins.mysql_model, id=mid
            )
            assert mysql_one.url == "http://testing.com"
        for pid in range(1, 11):
            postgres_one = await spider_ins.postgres_manager.get(
                spider_ins.postgres_model, id=pid
            )
            assert postgres_one.url == "http://testing.com"
        spider_ins.mysql_model.truncate_table()
        spider_ins.postgres_model.truncate_table()

    @pytest.mark.dependency(depends=["TestBoth::test_both_update"])
    async def test_both_dont_create_when_not_exists(
        self, mysql, postgresql, event_loop
    ):
        mysql, postgresql = basic_setup(mysql, postgresql)
        mmodel, _ = create_model(create_table=True, mysql=mysql)
        pmodel, _ = create_model(create_table=True, postgres=postgresql)
        mrows_before = mmodel.select().count()
        prows_before = pmodel.select().count()
        assert mrows_before == 0
        assert prows_before == 0
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
            create_when_not_exists=False,
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(1)
        mrows_after = await spider_ins.mysql_manager.count(
            spider_ins.mysql_model.select()
        )
        assert mrows_after == 0
        prows_after = await spider_ins.postgres_manager.count(
            spider_ins.postgres_model.select()
        )
        assert prows_after == 0

    @pytest.mark.dependency(depends=["TestBoth::test_both_dont_create_when_not_exists"])
    async def test_both_create_when_not_exists(self, mysql, postgresql, event_loop):
        mysql, postgresql = basic_setup(mysql, postgresql)
        pmodel, _ = create_model(create_table=True, postgres=postgresql)
        mmodel, _ = create_model(create_table=True, mysql=mysql)
        prows_before = pmodel.select().count()
        mrows_before = mmodel.select().count()
        assert prows_before == 0
        assert mrows_before == 0
        spider_ins = await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
        )
        while not spider_ins.request_session.closed:
            await asyncio.sleep(1)
        prows_after = 0
        while prows_after == 0:
            prows_after = await spider_ins.postgres_manager.count(
                spider_ins.postgres_model.select()
            )
            await asyncio.sleep(1)
        assert prows_after == 10
        mrows_after = 0
        while mrows_after == 0:
            mrows_after = await spider_ins.mysql_manager.count(
                spider_ins.mysql_model.select()
            )
            await asyncio.sleep(1)
        assert mrows_after == 10

    @pytest.mark.dependency(depends=["TestBoth::test_both_create_when_not_exists"])
    async def test_both_before_stop(self, mysql, postgresql, event_loop, caplog):
        mysql, postgresql = basic_setup(mysql, postgresql)
        await BothInsert.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
            before_stop=before_stop,
        )
        await BothUpdate.async_start(
            loop=event_loop,
            after_start=after_start(mysql=mysql, postgres=postgresql),
            target_db=TargetDB.BOTH,
            before_stop=before_stop,
        )
        assert "RuntimeError" not in caplog.text
        assert "Exception" not in caplog.text
