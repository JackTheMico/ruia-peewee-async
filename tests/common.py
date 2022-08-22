# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
from peewee import CharField, Model
from ruia import AttrField, Item, Response, Spider, TextField


class HackerNewsItem(Item):
    target_item = TextField(css_select="tr.item")
    title = AttrField(css_select="a.nbg", attr="title")
    url = AttrField(css_select="a.nbg", attr="href")

    async def clean_title(self, value):
        return value.strip()


class HackerNewsSpider(Spider):
    start_urls = ["https://movie.douban.com/chart"]
    # aiohttp_kwargs = {"proxy": "http://127.0.0.1:7890"}

    async def parse(self, response: Response):
        async for item in HackerNewsItem.get_items(html=await response.text()):
            yield item


class ResultModel(Model):

    title = CharField()
    url = CharField()
