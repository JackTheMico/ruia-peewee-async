# ruia-peewee-async
[![996.icu](https://img.shields.io/badge/link-996.icu-red.svg)](https://996.icu)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)

A [Ruia](https://github.com/howie6879/ruia) plugin that uses [peewee-async](https://github.com/05bit/peewee-async) to store data to MySQL or PostgreSQL or both of them.


## Installation

Using [pip](https://pip.pypa.io/en/stable/) or [ pipenv ](https://pipenv.pypa.io/en/latest/) or [ poetry ](https://python-poetry.org/) to install.

```shell
pip install ruia-peewee-async[aiomysql]
pipenv install ruia-peewee-async[aiomysql]
poetry add ruia-peewee-async[aiomysql]

or

pip install ruia-peewee-async[aiopg]
pipenv install ruia-peewee-async[aiopg]
poetry add ruia-peewee-async[aiopg]

or

pip install ruia-peewee-async[all]
pipenv install ruia-peewee-async[all]
poetry install ruia-peewee-async[all]
```
`ruia-peewee-async[all]` means to install both aiomysql and aiopg.

## Usage

A complete example is like below.
```python
# -*- coding: utf-8 -*-
from peewee import CharField
from ruia import AttrField, Item, Response, TextField

from ruia_peewee_async import (
    RuiaPeeweeInsert,
    RuiaPeeweeUpdate,
    Spider,
    TargetDB,
    after_start,
)

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
            # yield RuiaPeeweeInsert(item.results, filters="url")  # use url field(column) to deduplicate, avoid unnecessary insert query executed.
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
            # query: A peewee's query or a dict to search for the target data in database.
            # database: The target database type.
            # filters: A str or List[str] of columns to avoid duplicate data and avoid unnecessary query execute.
            # create_when_not_exists: Default is True. If True, will create a record when query can't get the record.
            # not_update_when_exists: Default is True. If True and record exists, won't update data to the records.
            # only: A list or tuple of fields that should be updated only.
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
```

There's a `create_model` method to create the Peewee model based on database configuration.
```python
from ruia_peewee_async import create_model

model = create_model(mysql=mysql) # or postgres=postgres or both
# create the table at the same time
model = create_mode(postgres=postgres, create_table=True)
rows = model.select().count()
print(rows)
```

And class `Spider` from `ruia_peewee_async` has attributes below related to database you can use.
```python
from peewee import Model
from typing import Dict
from peewee_async import Manager, MySQLDatabase, PostgresqlDatabase
from ruia import Spider as RuiaSpider

class Spider(RuiaSpider):
    mysql_model: Union[Model, Dict] # It will be a Model instance after spider started.
    mysql_manager: Manager
    postgres_model: Union[Model, Dict] # same above
    postgres_manager: Manager
    mysql_db: MySQLDatabase
    postgres_db: PostgresqlDatabase
```
For more information, check out [peewee's documentation](http://docs.peewee-orm.com/en/latest/) and [peewee-async's documentation](https://peewee-async.readthedocs.io/en/latest/).

## Development
Using `pyenv` to install the version of python that you need.
For example
```shell
pyenv install 3.7.9
```
Then go to the root of the project and run:
```shell
poetry install && poetry install -E aiomysql -E aiopg
```
to install all dependencies.

- Using `poetry shell` to enter the virtual environment.
  Or open your favorite editor and select the virtual environment to start coding.
- Using `pytest` to run unit tests under `tests` folder.
- Using `pytest --cov .` to run all tests and generate coverage report in terminal.

## Thanks
- [ruia](https://github.com/howie6879/ruia)
- [peewew](https://github.com/coleifer/peewee)
- [peewee-async](https://github.com/05bit/peewee-async)
- [aiomysql](https://github.com/aio-libs/aiomysql)
- [aiopg](https://github.com/aio-libs/aiopg)
- [schema](https://github.com/keleshev/schema)
- [pytest and its awesome plugins](https://github.com/pytest-dev/pytest)
