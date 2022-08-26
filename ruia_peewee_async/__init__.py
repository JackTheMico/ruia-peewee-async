# -*- coding: utf-8 -*-
__version__ = "1.0.4"
from copy import deepcopy
from enum import Enum
from types import MethodType
from typing import Dict, Optional, Union, Sequence, Tuple

from peewee import DoesNotExist, Model, Query
from peewee_async import Manager, MySQLDatabase, PostgresqlDatabase
from pymysql import OperationalError
from ruia import Spider as RuiaSpider


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
    async def process(spider_ins: RuiaSpider, callback_result):
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
                raise ParameterError(f"TargetDB value error: {database}")
        except OperationalError as ope:
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} insert error: {ope}>"
            )


class RuiaPeeweeUpdate:
    """Ruia Peewee Update Class"""

    def __init__(
        self,
        data: Dict,
        query: Union[Query, dict],
        database: TargetDB = TargetDB.MYSQL,
        create_when_not_exists: bool = True,
        not_update_when_exists: bool = True,
        only: Optional[Sequence[str]] = None,
    ) -> None:
        """

        Args:
            data: A dict that's going to be updated in the database.
            query: A peewee query or a dict to search for the target data in database.
            database: The target database type.
            create_when_not_exists: Default is True. If True, will create a record when data not exists.
            not_update_when_exists: Default is True. If True and record exists, won't update data to records.
            only: A list or tuple of fields that should be updated.

        """

        self.data = data
        self.query = query
        self.database = database
        self.create_when_not_exists = create_when_not_exists
        self.not_update_when_exists = not_update_when_exists
        self.only = only

    @staticmethod
    async def _deal_update(
        spider_ins,
        data,
        query,
        create_when_not_exists,
        not_update_when_exists,
        only,
        databases,
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
                if not_update_when_exists:
                    continue
                model_ins.__data__.update(data)
                await manager.update(model_ins, only=only)

    @staticmethod
    async def _update(
        spider_ins,
        data,
        query,
        database,
        create_when_not_exists,
        not_update_when_exists,
        only,
    ):
        if database == TargetDB.BOTH:
            databases = [TargetDB.MYSQL.name, TargetDB.POSTGRES.name]
        elif database in [TargetDB.MYSQL, TargetDB.POSTGRES]:
            databases = [database.name]
        else:
            raise ParameterError(f"TargetDB value error: {database}")
        await RuiaPeeweeUpdate._deal_update(
            spider_ins,
            data,
            query,
            create_when_not_exists,
            not_update_when_exists,
            only,
            databases,
        )

    @staticmethod
    async def process(spider_ins, callback_result):
        data = callback_result.data
        database = callback_result.database
        query = callback_result.query
        create_when_not_exists = callback_result.create_when_not_exists
        not_update_when_exists = callback_result.not_update_when_exists
        only = callback_result.only
        if not isinstance(query, (Query, dict)):
            raise ParameterError(
                f"Parameter 'query': {query} has to be a peewee.Query or a dict."
            )
        if only and not isinstance(only, Sequence):
            raise ParameterError(
                f"Parameter 'only': {only} has to be a Tuple or a List."
            )
        try:
            await RuiaPeeweeUpdate._update(
                spider_ins,
                data,
                query,
                database,
                create_when_not_exists,
                not_update_when_exists,
                only,
            )
        except OperationalError as ope:
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} insert error: {ope}>"
            )


def init_spider(*, spider_ins: Spider):
    mysql_config = getattr(spider_ins, "mysql_config", {})
    postgres_config = getattr(spider_ins, "postgres_config", {})
    create_model(
        spider_ins=spider_ins,
        create_table=True,
        mysql=mysql_config,
        postgres=postgres_config,
    )
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


def check_database_config(config: Dict):
    if not config:
        return
    keys = {
        "user": str,
        "password": str,
        "host": str,
        "port": int,
        "database": str,
        "model": Dict,
    }
    conf_keys = list(config.keys())
    for key, vtype in keys.items():
        if key not in conf_keys:
            raise ParameterError(f"{key} must in config dict.")
        value = config[key]
        if not isinstance(value, vtype):
            raise ParameterError(f"{key}'s type must be {vtype}")
        if key == "model":
            if "table_name" not in value.keys():
                raise ParameterError(f"{key} must in model dict")
            if not isinstance(value["table_name"], str):
                raise ParameterError(f"{key}'s table_name's value must be a str")


def check_config(kwargs) -> Sequence[Dict]:
    no_config_msg = """
            RuiaPeeweeAsync must have a param named mysql_config or postgres_config or both, eg:
            mysql_config = {
                'user': 'yourusername',
                'password': 'yourpassword',
                'host': '127.0.0.1',
                'port': 3306,
                'database': 'ruia_mysql',
                'model': {{
                    'table_name': 'ruia_mysql_table',
                    "title": CharField(),
                    'url': CharField(),
                }},
            }
            postgres_config = {
                'user': 'yourusername',
                'password': 'yourpassword',
                'host': '127.0.0.1',
                'port': 5432,
                'database': 'ruia_postgres',
                'model': {{
                    'table_name': 'ruia_postgres_table',
                    "title": CharField(),
                    'url': CharField(),
                }},
            }
            """
    if not kwargs:
        raise ParameterError(no_config_msg)
    mysql = kwargs.get("mysql", {})
    postgres = kwargs.get("postgres", {})
    if not mysql and not postgres:
        raise ParameterError(no_config_msg)
    check_database_config(mysql)
    check_database_config(postgres)
    mysql_model = mysql.get("model", None)
    postgres_model = postgres.get("model", None)
    raise_no_model(mysql, mysql_model, "MySQL")
    raise_no_model(postgres, postgres_model, "PostgreSQL")
    return mysql, mysql_model, postgres, postgres_model


def after_start(**kwargs):
    mysql, mysql_model, postgres, postgres_model = check_config(kwargs)

    async def init_after_start(spider_ins):

        if mysql and mysql_model:
            spider_ins.mysql_config = mysql
            spider_ins.mysql_model = mysql_model
        if postgres and postgres_model:
            spider_ins.postgres_config = postgres
            spider_ins.postgres_model = postgres_model
        init_spider(spider_ins=spider_ins)

    return init_after_start


def create_model(spider_ins=None, create_table=False, **kwargs) -> Tuple:
    kwcopy = deepcopy(kwargs)
    mysql, mysql_model, postgres, postgres_model = check_config(kwcopy)
    mysql_manager, postgres_manager = None, None
    if mysql:
        mysql.pop("model")
        mysql_db = MySQLDatabase(**mysql)
        mysql_manager = Manager(mysql_db)
        meta = type("Meta", (object,), {"database": mysql_db})
        table_name = mysql_model.pop("table_name")
        mysql_model["Meta"] = meta
        mysql_model = type(table_name, (Model,), mysql_model)
        if spider_ins:
            spider_ins.mysql_db = mysql_db
            spider_ins.mysql_model = mysql_model
            spider_ins.mysql_manager = mysql_manager
        if create_table:
            with mysql_manager.allow_sync():
                mysql_model.create_table(True)
    if postgres:
        postgres.pop("model")
        postgres_db = PostgresqlDatabase(**postgres)
        postgres_manager = Manager(postgres_db)
        meta = type("Meta", (object,), {"database": postgres_db})
        table_name = postgres_model.pop("table_name")
        postgres_model["Meta"] = meta
        postgres_model = type(table_name, (Model,), postgres_model)
        if spider_ins:
            spider_ins.postgres_db = postgres_db
            spider_ins.postgres_model = postgres_model
            spider_ins.postgres_manager = postgres_manager
        if create_table:
            with postgres_manager.allow_sync():
                postgres_model.create_table(True)
    if mysql and not postgres:
        return mysql_model, mysql_manager
    if postgres and not mysql:
        return postgres_model, postgres_manager
    return mysql_model, mysql_manager, postgres_model, postgres_manager
