# -*- coding: utf-8 -*-
from logging import getLogger
from random import randint

import pytest
from peewee import CharField

from ruia_peewee_async import RuiaPeeweeInsert, RuiaPeeweeUpdate, init_spider

from .common import HackerNewsSpider

logger = getLogger(__name__)


class MySQLInsert(HackerNewsSpider):
    async def parse(self, response):
        async for item in super().parse(response):
            yield RuiaPeeweeInsert(item.results)


class MySQLUpdate(HackerNewsSpider):
    async def parse(self, response):
        async for item in super().parse(response):
            res = {}
            res["title"] = item.results["title"]
            res["url"] = "http://testing.com"
            yield RuiaPeeweeUpdate(res, {"title": res["title"]})


def base_setup(mysql):
    async def init_after_start(spider_ins):
        spider_ins.mysql_config = mysql
        spider_ins.mysql_model = {
            "table_name": "ruia_mysql_test",
            "title": CharField(),
            "url": CharField(),
        }
        init_spider(spider_ins=spider_ins)

    return init_after_start


@pytest.mark.dependency()
async def test_mysql_insert(mysql, event_loop):
    after_start = base_setup(mysql)
    spider_ins = await MySQLInsert.async_start(loop=event_loop, after_start=after_start)
    count = await spider_ins.mysql_manager.count(spider_ins.mysql_model.select())
    one = await spider_ins.mysql_manager.get(spider_ins.mysql_model, id=randint(1, 11))
    one_msg = f"One data, title: {one.title}, url: {one.url}"
    logger.info(one_msg)
    assert count >= 10, "Should insert 10 rows in MySQL."


@pytest.mark.dependency(depends=["test_mysql_insert"])
async def test_mysql_update(mysql, event_loop):
    after_start = base_setup(mysql)
    spider_ins = await MySQLUpdate.async_start(loop=event_loop, after_start=after_start)
    one = await spider_ins.mysql_manager.get(spider_ins.mysql_model, id=randint(1, 11))
    one_msg = f"One data, title: {one.title}, url: {one.url}"
    logger.info(one_msg)
    assert one.url == "http://testing.com"
