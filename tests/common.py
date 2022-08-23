# -*- coding: utf-8 -*-
import typing

from ruia import AttrField, Item, Middleware, Response, TextField

from ruia_peewee_async import RuiaPeeweeInsert, RuiaPeeweeUpdate, Spider, TargetDB


class DoubanItem(Item):
    target_item = TextField(css_select="tr.item")
    title = AttrField(css_select="a.nbg", attr="title")
    url = AttrField(css_select="a.nbg", attr="href")

    async def clean_title(self, value):
        return value.strip()


class DoubanSpider(Spider):
    start_urls = ["https://movie.douban.com/chart"]
    # aiohttp_kwargs = {"proxy": "http://127.0.0.1:7890"}

    async def parse(self, response: Response):
        async for item in DoubanItem.get_items(html=await response.text()):
            yield item


class Insert(DoubanSpider):
    def __init__(
        self,
        middleware: typing.Union[typing.Iterable, Middleware] = None,
        loop=None,
        is_async_start: bool = False,
        cancel_tasks: bool = True,
        target_db: TargetDB = TargetDB.MYSQL,
        **spider_kwargs,
    ):
        self.target_db = target_db
        super().__init__(
            middleware, loop, is_async_start, cancel_tasks, **spider_kwargs
        )

    async def parse(self, response):
        async for item in super().parse(response):
            yield RuiaPeeweeInsert(item.results, database=self.target_db)


class Update(DoubanSpider):
    def __init__(
        self,
        middleware: typing.Union[typing.Iterable, Middleware] = None,
        loop=None,
        is_async_start: bool = False,
        cancel_tasks: bool = True,
        target_db: TargetDB = TargetDB.MYSQL,
        **spider_kwargs,
    ):
        self.target_db = target_db
        super().__init__(
            middleware, loop, is_async_start, cancel_tasks, **spider_kwargs
        )

    async def parse(self, response):
        async for item in super().parse(response):
            res = {}
            res["title"] = item.results["title"]
            res["url"] = "http://testing.com"
            yield RuiaPeeweeUpdate(res, {"title": res["title"]}, self.target_db)
