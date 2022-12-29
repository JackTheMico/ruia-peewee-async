# ruia-peewee-async
[![996.icu](https://img.shields.io/badge/link-996.icu-red.svg)](https://996.icu)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)

[![Testing on main branch](https://github.com/JackTheMico/ruia-peewee-async/actions/workflows/codecov.yml/badge.svg?branch=main)](https://github.com/JackTheMico/ruia-peewee-async/actions/workflows/codecov.yml)
[![Testing on develop branch](https://github.com/JackTheMico/ruia-peewee-async/actions/workflows/codecov.yml/badge.svg?branch=develop)](https://github.com/JackTheMico/ruia-peewee-async/actions/workflows/codecov.yml)

[![Semantic Release](https://github.com/JackTheMico/ruia-peewee-async/actions/workflows/release.yml/badge.svg)](https://github.com/JackTheMico/ruia-peewee-async/actions/workflows/release.yml)

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

A complete example is in [the example directory](./examples/douban.py).

There's a `create_model` method to create the Peewee model based on database configuration.
You can use the `create_model` method to manipulate tables before starting the spider.
```python
from ruia_peewee_async import create_model

mysql_model, mysql_manager, postgres_model, postgres_manager = create_model(mysql=mysql) # or postgres=postgres or both
# create the table at the same time
mysql_model, mysql_manager, postgres_model, postgres_manager = create_model(mysql=mysql, create_table=True) # or postgres=postgres or both
rows = mysql_model.select().count()
print(rows)
```

And class `Spider` from `ruia_peewee_async` has attributes below related to database you can use.
```python
from peewee import Model
from typing import Callable, Dict
from typing import Optional as TOptional
from peewee_async import (
    AsyncQueryWrapper,
    Manager,
    MySQLDatabase,
    PooledMySQLDatabase,
    PooledPostgresqlDatabase,
    PostgresqlDatabase,
)
from ruia import Spider as RuiaSpider

class Spider(RuiaSpider):
    mysql_model: Union[Model, Dict] # It will be a Model instance after spider started.
    mysql_manager: Manager
    postgres_model: Union[Model, Dict] # same above
    postgres_manager: Manager
    mysql_db: MySQLDatabase
    postgres_db: PostgresqlDatabase
    mysql_filters: TOptional[AsyncQueryWrapper]
    postgres_filters: TOptional[AsyncQueryWrapper]
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

MacOS users have to run `brew install postgresql` to install postgresql and export the `pg_config` to the PATH,
so that the `psycorg2` dependency can be installed successfully with pip.

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
