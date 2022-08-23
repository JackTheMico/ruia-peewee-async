# -*- coding: utf-8 -*-
from enum import Enum
from types import MethodType
from typing import Dict, List, Optional, Tuple, Union

from peewee import DoesNotExist, Model, Query
from peewee_async import Manager, MySQLDatabase, PostgresqlDatabase
from pymysql import OperationalError
from ruia import Spider as RuiaSpider
from ruia.exceptions import SpiderHookError


class Spider(RuiaSpider):
    mysql_model: Union[Model, Dict]
    mysql_manager: Manager
    postgres_model: Union[Model, Dict]
    postgres_manager: Manager
    mysql_db: MySQLDatabase
    postgres_db: PostgresqlDatabase


class TargetDB(Enum):
    MYSQL = 0
    POSTGRES = 1
    BOTH = 2


class ParameterError(Exception):
    pass


class RuiaPeeweeInsert:
    def __init__(self, data: Dict, database: TargetDB = TargetDB.MYSQL) -> None:
        """

        Args:
            data: A data that's going to be inserted into the database.
            database: The target database type.

        """

        self.data = data
        self.database = database

    @staticmethod
    async def process(spider_ins, callback_result):
        data = callback_result.data
        database = callback_result.database
        try:
            if database == TargetDB.MYSQL:
                await spider_ins.mysql_manager.create(spider_ins.mysql_model, **data)
            elif database == TargetDB.POSTGRES:
                await spider_ins.postgres_manager.create(
                    spider_ins.postgres_model, **data
                )
            elif database == TargetDB.BOTH:
                await spider_ins.mysql_manager.create(spider_ins.mysql_model, **data)
                await spider_ins.postgres_manager.create(
                    spider_ins.postgres_model, **data
                )
            else:
                raise ValueError(f"TargetDB Enum value error: {database}")
        except OperationalError as ope:
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} insert error: {ope}>"
            )
        except SpiderHookError as she:
            spider_ins.logger.error(f"SpiderHookError: {she}>")


class RuiaPeeweeUpdate:
    """Ruia Peewee Update Class"""

    def __init__(
        self,
        data: Dict,
        query: Union[Query, dict],
        database: TargetDB = TargetDB.MYSQL,
        create_when_not_exists: bool = True,
        only: Optional[Union[Tuple[str], List[str]]] = None,
    ) -> None:
        """

        Args:
            data: A dict that's going to be updated in the database.
            query: A peewee query or a dict to search for the target data in database.
            database: The target database type.
            create_when_not_exists: If True, will create a record when data not exists. Default is True.
            only: A list or tuple of fields that should be updated.

        """

        self.data = data
        self.query = query
        self.database = database
        self.create_when_not_exists = create_when_not_exists
        self.only = only

    @staticmethod
    async def _deal_update(
        spider_ins, data, query, create_when_not_exists, only, databases
    ):
        for database in databases:
            database = database.lower()
            manager: Manager = getattr(spider_ins, f"{database}_manager")
            model: Model = getattr(spider_ins, f"{database}_model")
            try:
                model_ins = await manager.get(model, **query)
            except DoesNotExist:
                if create_when_not_exists:
                    await manager.create(model, **data)
            else:
                model_ins.__data__.update(data)
                await manager.update(model_ins, only=only)

    @staticmethod
    async def _update(spider_ins, data, query, database, create_when_not_exists, only):
        if database == TargetDB.BOTH:
            databases = [TargetDB.MYSQL.name, TargetDB.POSTGRES.name]
        else:
            databases = [database.name]
        await RuiaPeeweeUpdate._deal_update(
            spider_ins, data, query, create_when_not_exists, only, databases
        )

    @staticmethod
    async def process(spider_ins, callback_result):
        data = callback_result.data
        database = callback_result.database
        query = callback_result.query
        create_when_not_exists = callback_result.create_when_not_exists
        only = callback_result.only
        if not isinstance(query, (Query, dict)):
            raise ParameterError(
                f"Parameter 'query': {query} has to be a peewee.Query or a dict."
            )
        if only and not isinstance(only, (Tuple, List)):
            raise ParameterError(
                f"Parameter 'only': {only} has to be a Tuple or a List."
            )
        try:
            await RuiaPeeweeUpdate._update(
                spider_ins, data, query, database, create_when_not_exists, only
            )
        except OperationalError as ope:
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} insert error: {ope}>"
            )
        except SpiderHookError as she:
            spider_ins.logger.error(f"SpiderHookError: {she}>")


def init_spider(*, spider_ins: Spider):
    mysql_config = getattr(spider_ins, "mysql_config", {})
    postgres_config = getattr(spider_ins, "postgres_config", {})
    if (
        (not mysql_config and not postgres_config)
        or (mysql_config and not isinstance(mysql_config, dict))
        or (postgres_config and not isinstance(postgres_config, dict))
    ):
        raise ValueError(
            """
            RuiaPeeweeAsync must have a param named mysql_config or postgres_config or both, eg:
            mysql_config = {
                'user': 'yourusername',
                'password': 'yourpassword',
                'host': '127.0.0.1',
                'port': 3306,
                'database': 'ruia_mysql'
            }
            postgres_config = {
                'user': 'yourusername',
                'password': 'yourpassword',
                'host': '127.0.0.1',
                'port': 5432,
                'database': 'ruia_postgres'
            }
            """
        )
    if mysql_config:
        spider_ins.mysql_db = MySQLDatabase(**mysql_config)
        spider_ins.mysql_manager = Manager(spider_ins.mysql_db)
        meta = type("Meta", (object,), {"database": spider_ins.mysql_db})
        table_name = spider_ins.mysql_model.pop("table_name")
        spider_ins.mysql_model["Meta"] = meta
        spider_ins.mysql_model = type(table_name, (Model,), spider_ins.mysql_model)
        with spider_ins.mysql_manager.allow_sync():
            spider_ins.mysql_model.create_table(True)
    if postgres_config:
        spider_ins.postgres_db = PostgresqlDatabase(**postgres_config)
        spider_ins.postgres_manager = Manager(spider_ins.postgres_db)
        meta = type("Meta", (object,), {"database": spider_ins.postgres_db})
        table_name = spider_ins.postgres_model.pop("table_name")
        spider_ins.postgres_model["Meta"] = meta
        spider_ins.postgres_model = type(
            table_name, (Model,), spider_ins.postgres_model
        )
        with spider_ins.postgres_manager.allow_sync():
            spider_ins.postgres_model.create_table(True)
    spider_ins.callback_result_map = spider_ins.callback_result_map or {}
    spider_ins.process_insert_callback_result = MethodType(
        RuiaPeeweeInsert.process, spider_ins
    )
    spider_ins.callback_result_map.update(
        {"RuiaPeeweeInsert": "process_insert_callback_result"}
    )
    spider_ins.process_update_callback_result = MethodType(
        RuiaPeeweeUpdate.process, spider_ins
    )
    spider_ins.callback_result_map.update(
        {"RuiaPeeweeUpdate": "process_update_callback_result"}
    )


def raise_no_model(config, model, name):
    if config and not model:
        raise ParameterError(
            f"""{name} must have 'model' in config and 'model' cannot be empty.
            For example:
                {{
                    'host': '127.0.0.1',
                    'port': 3306,
                    'user': 'ruiamysql',
                    'password': 'abc123',
                    'database': 'ruiamysql',
                    'model': {{
                        'table_name': 'ruia_mysql',
                        "title": CharField(),
                        'url': CharField(),
                    }},
                }}
                """
        )


def after_start(**kwargs):
    if not kwargs:
        raise ParameterError(
            "There must be a 'mysql' or 'postgres' parameter or both of them."
        )
    mysql = kwargs.get("mysql", {})
    postgres = kwargs.get("postgres", {})
    if not mysql and not postgres:
        raise ParameterError(
            "MySQL and PostgreSQL configs cannout be empty at the same time."
        )
    mysql_model = mysql.pop("model", None)
    postgres_model = postgres.pop("model", None)
    raise_no_model(mysql, mysql_model, "MySQL")
    raise_no_model(postgres, postgres_model, "PostgreSQL")

    async def init_after_start(spider_ins):
        if mysql and mysql_model:
            spider_ins.mysql_config = mysql
            spider_ins.mysql_model = mysql_model
        if postgres and postgres_model:
            spider_ins.postgres_config = postgres
            spider_ins.postgres_model = postgres_model
        init_spider(spider_ins=spider_ins)

    return init_after_start
