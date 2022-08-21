# -*- coding: utf-8 -*-
from peewee import CharField
from peewee import Model
from ruia import AttrField
from ruia import Item
from ruia import Response
from ruia import Spider
from ruia import TextField

from ruia_peewee_async import init_spider


class HackerNewsItem(Item):
    target_item = TextField(css_select="tr.athing")
    title = TextField(css_select="a.titlelink")
    url = AttrField(css_select="a.titlelink", attr="href")

    async def clean_title(self, value):
        return value.strip()


class HackerNewsSpider(Spider):
    start_urls = ["https://news.ycombinator.com/news?p=1"]
    aiohttp_kwargs = {"proxy": "http://127.0.0.1:7890"}

    async def parse(self, response: Response):
        async for item in HackerNewsItem.get_items(html=await response.text()):
            yield item


class ResultModel(Model):

    title = CharField()
    url = CharField()


async def init_after_start(spider_ins):
    spider_ins.mysql_config = {
        "user": "dlwxxxdlw",
        "password": "",
        "host": "128.0.0.1",
        "port": 3306,
        "database": "ruia_peewee_async",
    }
    spider_ins.postgres_config = {
        "user": "postgres",
        "password": "",
        "host": "127.0.0.1",
        "port": 5432,
        "database": "ruia_peewee_async",
    }
    spider_ins.mysql_model = {
        "table_name": "ruia_mysql_test",
        "title": CharField(),
        "url": CharField(),
    }
    spider_ins.postgres_model = {
        "table_name": "ruia_postgres_test",
        "title": CharField(),
        "url": CharField(),
    }
    init_spider(spider_ins=spider_ins)
