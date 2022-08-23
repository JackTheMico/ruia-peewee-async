# -*- coding: utf-8 -*-
from peewee import CharField
from ruia import AttrField, Item, Response, Spider, TextField

from ruia_peewee_async import RuiaPeeweeInsert, RuiaPeeweeUpdate, TargetDB, after_start


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
            yield RuiaPeeweeInsert(item.results)  # default is MySQL
            # yield RuiaPeeweeInsert(item.results, database=TargetDB.POSTGRES) # save to Postgresql
            # yield RuiaPeeweeInsert(item.results, database=TargetDB.BOTH) # save to both MySQL and Postgresql


class DoubanUpdateSpider(Spider):
    start_urls = ["https://movie.douban.com/chart"]

    async def parse(self, response: Response):
        async for item in DoubanItem.get_items(html=await response.text()):
            res = {}
            res["title"] = item.results["title"]
            res["url"] = "http://whatever.youwanttoupdate.com"
            yield RuiaPeeweeUpdate(
                res,
                {"title": res["title"]},
                database=TargetDB.POSTGRES,  # default is MySQL
            )

            # Args for RuiaPeeweeUpdate
            # data: A dict that's going to be updated in the database.
            # query: A peewee query or a dict to search for the target data in database.
            # database: The target database type.
            # create_when_not_exists: If True, will create a record when data not exists. Default is True.
            # only: A list or tuple of fields that should be updated.


mysql = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "ruiamysql",
    "password": "abc123",
    "database": "ruiamysql",
    "model": {
        "table_name": "ruia_mysql",
        "title": CharField(),
        "url": CharField(),
    },
}
postgres = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "ruiapostgres",
    "password": "abc123",
    "database": "ruiapostgres",
    "model": {
        "table_name": "ruia_postgres",
        "title": CharField(),
        "url": CharField(),
    },
}

if __name__ == "__main__":
    DoubanSpider.start(after_start=after_start(mysql=mysql))
    # DoubanSpider.start(after_start=after_start(postgres=postgres))
    # DoubanSpider.start(after_start=after_start(mysql=mysql, postgres=postgres))
    # DoubanUpdateSpider.start(after_start=after_start(mysql=mysql))
