# -*- coding: utf-8 -*-
from logging import getLogger

from ruia_peewee_async import RuiaPeeweeInsert, RuiaPeeweeUpdate

from .common import HackerNewsSpider, init_after_start

logger = getLogger(__name__)


class MySQLInsert(HackerNewsSpider):
    async def parse(self, response):
        async for item in super().parse(response):
            yield RuiaPeeweeInsert(item.results)


class MySQLUpdate(HackerNewsSpider):
    async def parse(self, response):
        async for item in super().parse(response):
            yield RuiaPeeweeUpdate(item.results, {"title": item.results["title"]})


def test_mysql_insert():
    MySQLInsert.start(after_start=init_after_start)


def test_mysql_update():
    MySQLUpdate.start(after_start=init_after_start)
