# -*- coding: utf-8 -*-
from enum import Enum
from types import MethodType
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from peewee import DoesNotExist
from peewee import Model
from peewee import Query
from peewee_async import Manager
from peewee_async import MySQLDatabase
from peewee_async import PostgresqlDatabase
from pymysql import OperationalError
from ruia import Spider
from ruia.exceptions import SpiderHookError


class TargetDB(Enum):
    MYSQL = 0
    POSTGRESQL = 1
    BOTH = 2


class ParameterError(Exception):
    pass


class RuiaPeeweeInsert:
    def __init__(self, data: Dict, database: TargetDB = TargetDB.MYSQL) -> None:
        self.data = data
        self.database = database

    @staticmethod
    async def process(spider_ins, callback_result):
        data = callback_result.data
        database = callback_result.database
        try:
            if database == TargetDB.MYSQL:
                await spider_ins.mysql_manager.create(spider_ins.mysql_model, **data)
            elif database == TargetDB.POSTGRESQL:
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
        self.data = data
        self.query = query
        self.database = database
        self.create_when_not_exists = create_when_not_exists
        self.only = only

    @staticmethod
    async def _update(spider_ins, data, database, query, create_when_not_exists, only):
        if database == TargetDB.MYSQL:
            try:
                model_ins = await spider_ins.mysql_manager.get(
                    spider_ins.mysql_model, **query
                )
            except DoesNotExist:
                if create_when_not_exists:
                    await spider_ins.mysql_manager.create(
                        spider_ins.mysql_model, **data
                    )
            else:
                model_ins.__data__.update(data)
                await spider_ins.mysql_manager.update(model_ins, only=only)
        elif database == TargetDB.POSTGRESQL:
            model_ins, created = await spider_ins.postgres_manager.get_or_create(
                spider_ins.postgres_model, query, defaults=data
            )
            if not created:
                model_ins.__data__ = data
                await spider_ins.postgres_manager.update(model_ins, only=only)
        elif database == TargetDB.BOTH:
            model_ins, created = await spider_ins.mysql_manager.get_or_create(
                spider_ins.mysql_model, query, defaults=data
            )
            if not created:
                model_ins.__data__ = data
                await spider_ins.mysql_manager.update(model_ins, only=only)
            model_ins, created = await spider_ins.postgres_manager.get_or_create(
                spider_ins.postgres_model, query, defaults=data
            )
            if not created:
                model_ins.__data__ = data
                await spider_ins.postgres_manager.update(model_ins, only=only)
        else:
            raise ValueError(f"TargetDB Enum value error: {database}")

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
                spider_ins, data, database, query, create_when_not_exists, only
            )
        except OperationalError as ope:
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} insert error: {ope}>"
            )
        except SpiderHookError as she:
            spider_ins.logger.error(f"SpiderHookError: {she}>")


def init_spider(*, spider_ins: Spider):
    mysql_config = getattr(spider_ins, "mysql_config", None)
    postgres_config = getattr(spider_ins, "postgres_config", None)
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
    # MySQL Insert
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
