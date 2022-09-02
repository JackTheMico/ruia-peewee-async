# -*- coding: utf-8 -*-

from copy import deepcopy

import pytest
from peewee import Model
from peewee_async import PooledMySQLDatabase, PooledPostgresqlDatabase
from schema import SchemaError, SchemaMissingKeyError

from ruia_peewee_async import after_start, create_model

from .common import Insert, RuiaPeeweeInsert, RuiaPeeweeUpdate, TargetDB, Update


@pytest.mark.no_cover
@pytest.fixture(scope="class")
def docker_setup():
    return False


@pytest.mark.no_cover
@pytest.fixture(scope="class")
def docker_cleansup():
    return False


class TestConfig:
    async def test_process_errconfig(
        self, event_loop
    ):  # pylint: disable=too-many-statements,too-many-locals
        class Temp:
            def __init__(
                self,
                data,
                database,
                query=None,
                filters=None,
                create_when_not_exists=True,
                not_update_when_exists=True,
                only=None,
            ):
                self.data = data
                self.database = database
                self.query = query
                self.filters = filters
                self.create_when_not_exists = create_when_not_exists
                self.not_update_when_exists = not_update_when_exists
                self.only = only

        with pytest.raises(SchemaError) as se1:
            insert = Insert(loop=event_loop)
            await RuiaPeeweeInsert.process(insert, Temp("testdata", TargetDB.MYSQL))
        assert se1.value.args[0] == (
            "<RuiaPeeweeAsync: insert process error: "
            "callback_result's data should be a <class 'dict'>>"
        )
        with pytest.raises(SchemaError) as se2:
            insert = Insert(loop=event_loop)
            await RuiaPeeweeInsert.process(insert, Temp({"hehe": "test"}, "mongo"))
        assert se2.value.args[0] == (
            "<RuiaPeeweeAsync: insert process error: "
            "callback_result's database should be a <enum 'TargetDB'>>"
        )
        with pytest.raises(SchemaError) as se3:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(update, Temp("testdata", TargetDB.POSTGRES))
        assert (
            se3.value.args[0]
            == "<RuiaPeeweeAsync: update process error: callback_result's data should be a <class 'dict'>>"
        )
        with pytest.raises(SchemaError) as se4:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(update, Temp({"hehe": "test"}, "errorbase"))
        assert (
            se4.value.args[0]
            == "<RuiaPeeweeAsync: update process error: callback_result's database should be a <enum 'TargetDB'>>"
        )
        with pytest.raises(SchemaError) as se5:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(
                update, Temp({"hehe": "test"}, TargetDB.BOTH, query="bad query")
            )

        assert se5.value.args[0] == (
            "<RuiaPeeweeAsync: update process error: callback_result's"
            " query should be a (<class 'peewee.Query'>, <class 'dict'>)>"
        )
        with pytest.raises(SchemaError) as se6:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(
                update, Temp({"hehe": "test"}, TargetDB.BOTH, query="bad query")
            )

        assert se6.value.args[0] == (
            "<RuiaPeeweeAsync: update process error: callback_result's"
            " query should be a (<class 'peewee.Query'>, <class 'dict'>)>"
        )
        with pytest.raises(SchemaError) as se7:
            insert = Insert(loop=event_loop)
            await RuiaPeeweeInsert.process(insert, Temp({}, TargetDB.MYSQL))
        assert (
            se7.value.args[0]
            == "<RuiaPeeweeAsync: insert process error: data cannot be empty>"
        )
        with pytest.raises(SchemaError) as se8:
            insert = Insert(loop=event_loop)
            await RuiaPeeweeUpdate.process(insert, Temp({}, TargetDB.MYSQL))

        assert (
            se8.value.args[0]
            == "<RuiaPeeweeAsync: update process error: data cannot be empty>"
        )
        with pytest.raises(SchemaError) as se9:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(
                update,
                Temp(
                    {"hehe": "test"},
                    TargetDB.BOTH,
                    query={"title": "test"},
                    create_when_not_exists="wrong type",
                ),
            )

        assert se9.value.args[0] == (
            "<RuiaPeeweeAsync: update process error: callback_result's"
            " create_when_not_exists should be a <class 'bool'>>"
        )
        with pytest.raises(SchemaError) as se10:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(
                update,
                Temp(
                    {"hehe": "test"},
                    TargetDB.BOTH,
                    query={"title": "test"},
                    not_update_when_exists=3397,
                ),
            )
        assert se10.value.args[0] == (
            "<RuiaPeeweeAsync: update process error: callback_result's"
            " not_update_when_exists should be a <class 'bool'>>"
        )
        with pytest.raises(SchemaError) as se11:
            update = Update(loop=event_loop)
            await RuiaPeeweeUpdate.process(
                update,
                Temp({"hehe": "test"}, TargetDB.BOTH, query={"title": "test"}, only={}),
            )
        assert se11.value.args[0] == (
            "<RuiaPeeweeAsync: update process error: callback_result's only "
            "should be a (<class 'list'>, <class 'tuple'>, <class 'NoneType'>)>"
        )

    async def test_noconfig(
        self, docker_setup, docker_cleanup, event_loop
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        with pytest.raises(SchemaMissingKeyError) as smke:
            await Insert.async_start(loop=event_loop, after_start=after_start())
        assert smke.value.args[0] == "Missing key: Or('mysql', 'postgres')"
        with pytest.raises(SchemaError) as se1:
            await Insert.async_start(loop=event_loop, after_start=after_start(mysql={}))
        assert (
            "Missing keys: 'database', 'host', 'model', 'password', 'user'"
            in se1.value.args[0]
        )
        assert se1.value.args[0].startswith("Key 'mysql' error")
        with pytest.raises(SchemaError) as se2:
            await Insert.async_start(
                loop=event_loop, after_start=after_start(postgres={})
            )
        assert (
            "Missing keys: 'database', 'host', 'model', 'password', 'user'"
            in se2.value.args[0]
        )
        assert se2.value.args[0].startswith("Key 'postgres' error")
        with pytest.raises(SchemaError) as se3:
            await Insert.async_start(
                loop=event_loop, after_start=after_start(postgres={}, mysql={})
            )
        assert (
            "Missing keys: 'database', 'host', 'model', 'password', 'user'"
            in se3.value.args[0]
        )
        assert se3.value.args[0].startswith("Key 'postgres' error")

    async def test_no_model(
        self, docker_setup, docker_cleanup, event_loop, mysql_config, postgres_config
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        mysql_config.pop("model")
        postgres_config.pop("model")
        with pytest.raises(SchemaError):
            await Update.async_start(
                loop=event_loop, after_start=after_start(mysql=mysql_config)
            )
        with pytest.raises(SchemaError):
            await Update.async_start(
                loop=event_loop, after_start=after_start(postgres=postgres_config)
            )

    async def test_err_config(
        self, docker_setup, docker_cleanup, event_loop, mysql_config, postgres_config
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        with pytest.raises(SchemaError) as se1:
            mysql = deepcopy(mysql_config)
            mysql.pop("password")
            await Insert.async_start(
                loop=event_loop, after_start=after_start(mysql=mysql)
            )
        assert "Missing key: 'password'" in se1.value.args[0]
        with pytest.raises(SchemaError) as se2:
            postgres = deepcopy(postgres_config)
            postgres["weird"] = "weird value"
            await Update.async_start(
                loop=event_loop, after_start=after_start(postgres=postgres)
            )
        assert "Wrong key 'weird' in" in se2.value.args[0]
        with pytest.raises(SchemaError) as se3:
            mysql = deepcopy(mysql_config)
            mysql["port"] = "werid value"
            await Insert.async_start(
                loop=event_loop, after_start=after_start(mysql=mysql)
            )
        assert "'werid value' should be instance of 'int'" in se3.value.args[0]
        with pytest.raises(SchemaError) as se4:
            postgres = deepcopy(postgres_config)
            postgres["model"].pop("table_name")
            await Update.async_start(
                loop=event_loop, after_start=after_start(postgres=postgres)
            )
        assert "Key 'model' error:\nMissing key: 'table_name'" in se4.value.args[0]

    async def test_pool_config(
        self,
        docker_setup,
        docker_cleanup,
        event_loop,
        pool_mysql_config,
        pool_postgres_config,
    ):  # pylint: disable=redefined-outer-name,unused-argument,unknown-option-value
        (  # pylint: disable=unbalanced-tuple-unpacking
            mysql_model,
            mysql_manager,
            postgres_model,
            postgres_manager,
        ) = create_model(mysql=pool_mysql_config, postgres=pool_postgres_config)
        assert isinstance(mysql_model, Model) is True
        assert isinstance(postgres_model, Model) is True
        assert isinstance(mysql_manager.database, PooledMySQLDatabase) is True
        assert isinstance(postgres_manager.database, PooledPostgresqlDatabase) is True
        assert mysql_manager.database.min_connections == 5
        assert mysql_manager.database.max_connections == 20
        assert postgres_manager.database.min_connections == 5
        assert postgres_manager.database.max_connections == 20
