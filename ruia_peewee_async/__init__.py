# -*- coding: utf-8 -*-
__version__ = "1.0.4"
from enum import Enum
from functools import wraps
from ssl import SSLContext
from types import MethodType
from typing import Dict
from typing import Optional as TOptional
from typing import Sequence, Tuple, Union

from peewee import DoesNotExist, Model, Query
from peewee_async import Manager, MySQLDatabase, PostgresqlDatabase
from pymysql import OperationalError
from ruia import Spider as RuiaSpider
from schema import And, Optional, Or, Schema, SchemaError, Use


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


def logging(func):
    @wraps(func)
    async def decorator(spider_ins: Spider, callback_result):
        data = callback_result.data
        database = callback_result.database
        msg_pre = f"<RuiaPeeweeAsync: Success insert data: {data} into "
        try:
            result = await func(spider_ins, callback_result)
        except OperationalError as ope:  # pragma: no cover
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} insert error: {ope}>"
            )
        except SchemaError as pae:
            spider_ins.logger.error(pae)
            raise pae
        else:
            msg = "".join([msg_pre, database.name, ">"])
            spider_ins.logger.info(msg)
            return result

    return decorator


def _raise_no_attr(target, fields, pre_msg):
    for field in fields:
        if hasattr(target, field):
            continue
        raise SchemaError(
            f"<{pre_msg} error: callback_result should have {field} attribute>"
        )


def _check_result(data: Tuple):
    target, type_dict, pre_msg = data
    type_dict: Dict
    _raise_no_attr(target, type_dict.keys(), pre_msg)
    for name, vtype in type_dict.items():
        msg = f"<{pre_msg} error: callback_result's {name} should be a {vtype}>"
        attr = getattr(target, name)
        if name in ["data", "query"] and not attr:
            raise SchemaError(f"<{pre_msg} error: {name} cannot be empty>")
        if not isinstance(attr, vtype):
            raise SchemaError(msg)


result_validator = Schema(Use(_check_result))


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
    @logging
    async def process(spider_ins: Spider, callback_result):
        needs_check = (
            callback_result,
            {"data": dict, "database": TargetDB},
            "RuiaPeeweeAsync: insert process",
        )
        result_validator.validate(needs_check)
        data = callback_result.data
        database = callback_result.database
        if database == TargetDB.MYSQL:
            await spider_ins.mysql_manager.create(spider_ins.mysql_model, **data)
        elif database == TargetDB.POSTGRES:
            await spider_ins.postgres_manager.create(spider_ins.postgres_model, **data)
        else:
            await spider_ins.mysql_manager.create(spider_ins.mysql_model, **data)
            await spider_ins.postgres_manager.create(spider_ins.postgres_model, **data)


class RuiaPeeweeUpdate:
    """Ruia Peewee Update Class"""

    def __init__(
        self,
        data: Dict,
        query: Union[Query, Dict],
        database: TargetDB = TargetDB.MYSQL,
        create_when_not_exists: bool = True,
        not_update_when_exists: bool = True,
        only: TOptional[Sequence[str]] = None,
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
                    spider_ins.logger.info(
                        f"<RuiaPeeweeAsync: data: {data} not exists in {database}, but success created>"
                    )
                else:
                    spider_ins.logger.warning(
                        f"<RuiaPeeweeAsync: data: {data} not exists in {database}, \
                                won't create it because create_when_not_exists is False>"
                    )
            else:
                if not_update_when_exists:
                    spider_ins.logger.info(
                        f"<RuiaPeeweeAsync: {data} won't updated in {database}>"
                    )
                    continue
                model_ins.__data__.update(data)
                await manager.update(model_ins, only=only)
                spider_ins.logger.info(
                    f"<RuiaPeeweeAsync: {data} was updated in {database}>"
                )

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
        else:
            databases = [database.name]
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
    @logging
    async def process(spider_ins, callback_result):
        data = callback_result.data
        database = callback_result.database
        query = callback_result.query
        create_when_not_exists = callback_result.create_when_not_exists
        not_update_when_exists = callback_result.not_update_when_exists
        only = callback_result.only
        needs_check = (
            callback_result,
            {
                "data": dict,
                "database": TargetDB,
                "query": (Query, dict),
                "create_when_not_exists": bool,
                "not_update_when_exists": bool,
                "only": (list, tuple, type(None)),
            },
            "RuiaPeeweeAsync: update process",
        )
        result_validator.validate(needs_check)
        await RuiaPeeweeUpdate._update(
            spider_ins,
            data,
            query,
            database,
            create_when_not_exists,
            not_update_when_exists,
            only,
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


def check_config(kwargs) -> Sequence[Dict]:
    # no_config_msg = """
    #         RuiaPeeweeAsync must have a param named mysql_config or postgres_config or both, eg:
    #         mysql_config = {
    #             'user': 'yourusername',
    #             'password': 'yourpassword',
    #             'host': '127.0.0.1',
    #             'port': 3306,
    #             'database': 'ruia_mysql',
    #             'model': {{
    #                 'table_name': 'ruia_mysql_table',
    #                 "title": CharField(),
    #                 'url': CharField(),
    #             }},
    #         }
    #         postgres_config = {
    #             'user': 'yourusername',
    #             'password': 'yourpassword',
    #             'host': '127.0.0.1',
    #             'port': 5432,
    #             'database': 'ruia_postgres',
    #             'model': {{
    #                 'table_name': 'ruia_postgres_table',
    #                 "title": CharField(),
    #                 'url': CharField(),
    #             }},
    #         }
    #         """
    conf_validator = Schema(
        {
            Or("mysql", "postgres"): Or(
                None,
                And(
                    {
                        "host": And(str),
                        "user": And(str),
                        "password": And(str),
                        "database": And(str),
                        "model": And({"table_name": And(str), str: object}),
                        Optional("port"): And(int),
                        Optional("ssl"): Use(SSLContext),
                    }
                ),
            )
        }
    )
    kwval = conf_validator.validate(kwargs)
    mysql = kwval.get("mysql", {})
    postgres = kwval.get("postgres", {})
    mysql_model = mysql.get("model", None)
    postgres_model = postgres.get("model", None)
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
    mysql, postgres = kwargs.get("mysql", {}), kwargs.get("postgres", {})
    mysql_mconf = mysql.get("model", {})
    postgres_mconf = postgres.get("model", {})
    mysql_model, mysql_manager, postgres_model, postgres_manager = (
        None,
        None,
        None,
        None,
    )
    if mysql:
        mysql_db = MySQLDatabase(
            **{key: val for key, val in mysql.items() if key != "model"}
        )
        mysql_manager = Manager(mysql_db)
        meta = type("Meta", (object,), {"database": mysql_db})
        table_name = mysql_mconf.pop("table_name")
        mysql_mconf["Meta"] = meta
        mysql_model = type(table_name, (Model,), mysql_mconf)
        if spider_ins:
            spider_ins.mysql_db = mysql_db
            spider_ins.mysql_model = mysql_model
            spider_ins.mysql_manager = mysql_manager
        if create_table:
            with mysql_manager.allow_sync():
                mysql_model.create_table(True)
        mysql_mconf["table_name"] = table_name
    if postgres:
        postgres_db = PostgresqlDatabase(
            **{key: val for key, val in postgres.items() if key != "model"}
        )
        postgres_manager = Manager(postgres_db)
        meta = type("Meta", (object,), {"database": postgres_db})
        table_name = postgres_mconf.pop("table_name")
        postgres_mconf["Meta"] = meta
        postgres_model = type(table_name, (Model,), postgres_mconf)
        if spider_ins:
            spider_ins.postgres_db = postgres_db
            spider_ins.postgres_model = postgres_model
            spider_ins.postgres_manager = postgres_manager
        if create_table:
            with postgres_manager.allow_sync():
                postgres_model.create_table(True)
        postgres_mconf["table_name"] = table_name
    if mysql and not postgres:
        return mysql_model, mysql_manager
    if postgres and not mysql:
        return postgres_model, postgres_manager
    return mysql_model, mysql_manager, postgres_model, postgres_manager
