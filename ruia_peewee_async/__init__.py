# -*- coding: utf-8 -*-
__version__ = "1.0.4"
from enum import Enum
from functools import wraps
from ssl import SSLContext
from types import MethodType
from typing import Callable, Dict
from typing import Optional as TOptional
from typing import Sequence, Tuple, Union

from peewee import DoesNotExist, Model, Query
from peewee_async import (
    AsyncQueryWrapper,
    Manager,
    MySQLDatabase,
    PooledMySQLDatabase,
    PooledPostgresqlDatabase,
    PostgresqlDatabase,
)
from pymysql import OperationalError
from ruia import Spider as RuiaSpider
from schema import And, Optional, Or, Schema, SchemaError, Use


class Spider(RuiaSpider):
    mysql_model: Model
    mysql_manager: Manager
    postgres_model: Model
    postgres_manager: Manager
    mysql_db: Union[MySQLDatabase, PooledMySQLDatabase]
    postgres_db: Union[PostgresqlDatabase, PooledPostgresqlDatabase]
    mysql_filters: TOptional[AsyncQueryWrapper]
    postgres_filters: TOptional[AsyncQueryWrapper]
    process_insert_callback_result: Callable
    process_update_callback_result: Callable


class TargetDB(Enum):
    MYSQL = 0
    POSTGRES = 1
    BOTH = 2


def logging(func):
    @wraps(func)
    async def decorator(spider_ins: Spider, callback_result):
        data = callback_result.data
        database = callback_result.database
        query = getattr(callback_result, "query", None)
        try:
            result = await func(spider_ins, callback_result)
        except OperationalError as ope:  # pragma: no cover
            method = "insert" if not query else "update"
            spider_ins.logger.error(
                f"<RuiaPeeweeAsync: {database.name} {method} data: {data} error: {ope}>"
            )
        except SchemaError as pae:
            spider_ins.logger.error(pae)
            raise pae
        else:
            spider_ins.logger.info(result)

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


async def filter_func(data, spider_ins, database, manager, model, filters) -> bool:
    if not hasattr(spider_ins, f"{database}_filters"):
        conditions = [getattr(model, fil) for fil in filters]
        filter_res = await manager.execute(model.select(*conditions).distinct())
        setattr(spider_ins, f"{database}_filters", filter_res)
    filtered = False
    filter_res = getattr(spider_ins, f"{database}_filters")
    for fil in filters:
        if data[fil] in [getattr(x, fil) for x in filter_res]:
            filtered = True
    return filtered


class RuiaPeeweeInsert:
    def __init__(
        self,
        data: Dict,
        database: TargetDB = TargetDB.MYSQL,
        filters: TOptional[Union[Sequence[str], str]] = None,
    ) -> None:
        """

        Args:
            data: A data that's going to be inserted into the database.
            database: The target database type.

        """

        self.data = data
        self.database = database
        self.filters = filters

    @staticmethod
    @logging
    async def process(spider_ins: Spider, callback_result):
        needs_check = (
            callback_result,
            {"data": dict, "database": TargetDB, "filters": (str, type(None), list)},
            "RuiaPeeweeAsync: insert process",
        )
        result_validator.validate(needs_check)
        data = callback_result.data
        database = callback_result.database
        filters = callback_result.filters
        if database == TargetDB.BOTH:
            databases = [TargetDB.MYSQL.name, TargetDB.POSTGRES.name]
        else:
            databases = [database.name]
        msg = ""
        if isinstance(filters, str):
            filters = [filters]
        for database in databases:
            database = database.lower()
            manager: Manager = getattr(spider_ins, f"{database}_manager")
            model: Model = getattr(spider_ins, f"{database}_model")
            if filters:
                filtered = await filter_func(
                    data, spider_ins, database, manager, model, filters
                )
                if filtered:
                    msg += (
                        f"<RuiaPeeweeAsync: data: {data} was filtered by filters: {filters},"
                        f" won't insert into {database.upper()}>\n"
                    )
                    continue
                msg += (
                    f"<RuiaPeeweeAsync: data: {data} wasn't filtered by filters: {filters}, "
                    f"success insert into {database.upper()}>\n"
                )
            await manager.create(model, **data)
        if msg:
            return msg
        return f"<RuiaPeeweeAsync: Success insert {data} into database: {databases}>"


class RuiaPeeweeUpdate:
    """Ruia Peewee Update Class"""

    def __init__(
        self,
        data: Dict,
        query: Union[Query, Dict],
        database: TargetDB = TargetDB.MYSQL,
        filters: TOptional[Union[Sequence[str], str]] = None,
        create_when_not_exists: bool = True,
        not_update_when_exists: bool = True,
        only: TOptional[Sequence[str]] = None,
    ) -> None:
        """

        Args:
            data: A dict that's going to be updated in the database.
            query: A peewee's query or a dict to search for the target data in database.
            database: The target database type.
            filters: A str or List[str] of columns to avoid duplicate data and avoid unnecessary query execute.
            create_when_not_exists: Default is True. If True, will create a record when query can't get the record.
            not_update_when_exists: Default is True. If True and record exists, won't update data to the records.
            only: A list or tuple of fields that should be updated only.

        """

        self.data = data
        self.query = query
        self.database = database
        self.filters = filters
        self.create_when_not_exists = create_when_not_exists
        self.not_update_when_exists = not_update_when_exists
        self.only = only

    @staticmethod
    async def _deal_update(
        spider_ins,
        data,
        query,
        filters,
        create_when_not_exists,
        not_update_when_exists,
        only,
        databases,
    ):  # pylint: disable=too-many-locals
        msg = ""
        if isinstance(filters, str):
            filters = [filters]
        for database in databases:
            database = database.lower()
            manager: Manager = getattr(spider_ins, f"{database}_manager")
            model: Model = getattr(spider_ins, f"{database}_model")
            if filters:
                filtered = await filter_func(
                    data, spider_ins, database, manager, model, filters
                )
                if filtered:
                    msg += f"<RuiaPeeweeAsync: data: {data} was filtered by filters: {filters}\n"
                    continue
                msg += f"<RuiaPeeweeAsync: data: {data} wasn't filtered by filters: {filters}\n"
            try:
                model_ins = await manager.get(model, **query)
            except DoesNotExist:
                if create_when_not_exists:
                    await manager.create(model, **data)
                    msg += f"<RuiaPeeweeAsync: data: {data} not exists in {database.upper()}, but success created>\n"
                msg += (
                    f"<RuiaPeeweeAsync: data: {data} not exists in {database.upper()}, "
                    "won't create it because create_when_not_exists is False>\n"
                )
            else:
                if not_update_when_exists:
                    msg += (
                        f"<RuiaPeeweeAsync: Won't update {data} in {database.upper()} "
                        "because not_update_when_exists is True>\n"
                    )
                    continue
                model_ins.__data__.update(data)
                await manager.update(model_ins, only=only)
        if msg:
            return msg
        return f"<RuiaPeeweeAsync: Updated {data} in {databases}>"

    @staticmethod
    async def _update(
        spider_ins,
        data,
        query,
        filters,
        database,
        create_when_not_exists,
        not_update_when_exists,
        only,
    ):
        if database == TargetDB.BOTH:
            databases = [TargetDB.MYSQL.name, TargetDB.POSTGRES.name]
        else:
            databases = [database.name]
        result = await RuiaPeeweeUpdate._deal_update(
            spider_ins,
            data,
            query,
            filters,
            create_when_not_exists,
            not_update_when_exists,
            only,
            databases,
        )
        return result

    @staticmethod
    @logging
    async def process(spider_ins, callback_result):
        data = callback_result.data
        database = callback_result.database
        query = callback_result.query
        filters = callback_result.filters
        create_when_not_exists = callback_result.create_when_not_exists
        not_update_when_exists = callback_result.not_update_when_exists
        only = callback_result.only
        needs_check = (
            callback_result,
            {
                "data": dict,
                "database": TargetDB,
                "query": (Query, dict),
                "filters": (str, type(None), list),
                "create_when_not_exists": bool,
                "not_update_when_exists": bool,
                "only": (list, tuple, type(None)),
            },
            "RuiaPeeweeAsync: update process",
        )
        result_validator.validate(needs_check)
        result = await RuiaPeeweeUpdate._update(
            spider_ins,
            data,
            query,
            filters,
            database,
            create_when_not_exists,
            not_update_when_exists,
            only,
        )
        return result


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
                        Optional("pool"): And(bool),
                        Optional("min_connections"): And(
                            int, lambda mic: 1 <= mic <= 10
                        ),
                        Optional("max_connections"): And(
                            int, lambda mac: 10 < mac <= 20
                        ),
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
            # spider_ins.mysql_model = mysql_model
        if postgres and postgres_model:
            spider_ins.postgres_config = postgres
            # spider_ins.postgres_model = postgres_model
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
        mysql_db = (
            PooledMySQLDatabase(
                **{
                    key: val
                    for key, val in mysql.items()
                    if key not in ("model", "pool")
                }
            )
            if "pool" in mysql
            else MySQLDatabase(
                **{key: val for key, val in mysql.items() if key != "model"}
            )
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
        postgres_db = (
            PooledPostgresqlDatabase(
                **{
                    key: val
                    for key, val in postgres.items()
                    if key not in ("model", "pool")
                }
            )
            if "pool" in postgres
            else PostgresqlDatabase(
                **{key: val for key, val in postgres.items() if key != "model"}
            )
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
