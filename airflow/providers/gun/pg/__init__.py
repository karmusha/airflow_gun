"""
Позволяет выполнить последовательные запросы используя один созданный connection к postgres
Все декораторы объявлены с префиксом @pg_
Цепочка декораторов должна:
 - начинаться с airflow декоратора с python_callable, например @task.python, @task.sensor
 - заканчиваться декоратором @pg_run

Примеры использования:
    # выполним запрос из первого файла
    # потом выполним запрос из второго файла
    # выполним коммит в конце 
    # используем airflow connection для подключения
    @task.python
    @pg_execute_file("{{ params.second_sql_file }}")
    @pg_execute_file("{{ params.first_sql_file }}")
    @pg_auth_conn_id("{{ params.gp_conn_id }}")
    @pg_run()
    def postgres(*args, **kwargs):
        conn = args[0]
        conn.commit()

    # выполним запрос 'copy to', скопируем результат в файл params.gzip_file (используя запрос из файла params.first_sql_file)
    # выполним запрос 'copy from' скопируем данные из файла params.gzip_file (используя запрос из файла params.second_sql_file)
    # выполним коммит в конце
    # используем airflow connection для подключения
    @task.python
    @pg_copy_from_gzip_use_sql_file(
        "{{ params.second_sql_file }}",
        "{{ params.gzip_file }}"
    )
    @pg_copy_to_gzip_use_sql_file(
        "{{ params.first_sql_file }}",
        "{{ params.gzip_file }}"
    )
    @pg_auth_conn_id("{{ params.gp_conn_id }}")
    @pipe("pg")
    def postgres(*args, **kwargs):
        conn = args[0]
        conn.commit()
"""

import os
import sys
import io
import gzip
import logging
from typing import (
    Any,
    Callable,
    Optional,
    TextIO,
    Type,
    Union,
    Dict,
)

from pathlib import Path
from contextlib import closing, ExitStack

import psycopg2
import psycopg2.extras
import psycopg2.sql

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder


__all__ = [
    "pg_execute",
    "pg_execute_and_commit",
    "pg_execute_file",
    "pg_execute_file_and_commit",
    "pg_fetch_to_stdout",
    "pg_commit",
    "pg_module",
    "pg_copy_to_handle",
    "pg_copy_to_stdout",
    "pg_copy_to_file",
    "pg_copy_to_gzip",
    "pg_copy_to_file_use_sql_file",
    "pg_copy_to_gzip_use_sql_file",
    "pg_copy_from_file",
    "pg_copy_from_gzip",
    "pg_copy_from_file_use_sql_file",
    "pg_copy_from_gzip_use_sql_file",
    "pg_register_uuid",
    "pg_register_json",
    "pg_register_inet",
    "pg_register_hstore",
    "pg_register_default_json",
    "pg_register_default_jsonb",
    "pg_register_composite",
    "pg_register_ipaddress",
    "pg_register_range",
    "pg_save_to_xcom",
    "get_postgres_logger",
]


class StringIteratorIO(io.TextIOBase):
    """на будущее"""

    def __init__(self, iterator):
        self._iter = iterator
        self._buff = ""

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret) :]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)


class PrintSqlCursor(psycopg2.extensions.cursor):
    """
    Курсор который принтует запросы в stdout
    """

    def execute(self, query, vars=None):
        try:
            print("---- query ----")
            print(self.connection.dsn)
            print(query)
            print(f"parameters: {vars}")
            res = super().execute(query, vars)
            print("---- query success ----")
            return res
        except Exception as e:
            print(f"---- query error: {e}", file=sys.stderr)
            raise

    def executemany(self, query, vars_list):
        try:
            print("---- query ----")
            print(self.connection.dsn)
            print(query)
            res = super().executemany(query, vars_list)
            print("---- query success ----")
            return res
        except Exception as e:
            print(f"---- query error: {e}", file=sys.stderr)
            raise

    def callproc(self, procname, vars=None):
        try:
            print(f"---- call {procname} ({vars}) ----")
            res = super().callproc(procname, vars)
            print("---- call success ----")
            return res
        except Exception as e:
            print(f"---- call error: {e}", file=sys.stderr)
            raise

    def copy_expert(self, sql, file, **kwargs):
        try:
            print(f"---- copy ----")
            print(self.connection.dsn)
            print(sql)
            res = super().copy_expert(sql, file, **kwargs)
            print("---- copy success ----")
            return res
        except Exception as e:
            print(f"---- copy error: {e}", file=sys.stderr)
            raise

    def copy_from(self, file, table, **kwargs):
        try:
            print(f"---- copy from ----")
            print(self.connection.dsn)
            print(file)
            print(table)
            res = super().copy_from(file, table, **kwargs)
            print("---- copy from success ----")
            return res
        except Exception as e:
            print(f"---- copy from error: {e}", file=sys.stderr)
            raise

    def copy_to(self, file, table, **kwargs):
        try:
            print(f"---- copy to ----")
            print(self.connection.dsn)
            print(file)
            print(table)
            res = super().copy_to(file, table, **kwargs)
            print("---- copy to success ----")
            return res
        except Exception as e:
            print(f"---- copy to error: {e}", file=sys.stderr)
            raise


class PostgresAuthAirflowConnection(PipeTask):
    def __init__(
        self,
        context_key: str,
        stack_key: str,
        template_render: Callable,
        conn_id: Optional[str] = None,
        dsn: Optional[str] = None,
        connection_factory: Optional[Callable] = None,
        cursor_name: Optional[str] = None,
        cursor_factory: Optional[Type[psycopg2.extensions.cursor]] = None,
        cursor_withhold: Optional[bool] = None,
        cursor_scrollable: Optional[bool] = None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "conn_id",
                "dsn",
                "cursor_name",
                "cursor_withhold",
                "cursor_scrollable",
            )
        )
        super().set_template_render(template_render)

        self.context_key = context_key
        self.stack_key = stack_key
        self.conn_id = conn_id
        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_name = cursor_name
        self.cursor_factory = cursor_factory
        self.cursor_withhold = cursor_withhold
        self.cursor_scrollable = cursor_scrollable

    @staticmethod
    def default_connection_factory(**conn):
        conn = psycopg2.connect(**conn)
        return conn

    def get_cur_with_stack(self, stack):
        if not self.connection_factory:
            self.connection_factory = (
                PostgresAuthAirflowConnection.default_connection_factory
            )

        conn = self.get_conn()
        conn = self.connection_factory(**conn)
        conn = stack.enter_context(closing(conn))
        cur = self.get_cur(conn)
        cur = stack.enter_context(closing(cur))

        return cur

    def get_airflow_conn(self, conn_id):
        try:
            return Connection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException as e:
            print(f"conn: {conn_id} not found. Please make sure it exists")
            raise e
        except Exception as e:
            raise e

    def get_conn_args_from_airflow(self, conn: Connection):
        """Преобразование Airflow connection в аргументы коннекшина для psycopg2
        драйвер psycopg2 принимает следующие параметры: https://github.com/psycopg/psycopg2/blob/master/psycopg/conninfo_type.c
        """
        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": conn.schema,
            "port": conn.port,
        }

        # любые опциональные параметры можно передать через блок extra в виде json
        if conn.extra_dejson:
            for name, val in conn.extra_dejson.items():
                conn_args[name] = val

        return conn_args

    def get_conn(self):
        if self.conn_id:
            conn_airflow = self.get_airflow_conn(self.conn_id)
            conn_airflow = self.get_conn_args_from_airflow(conn_airflow)
        else:
            conn_airflow = {}

        if self.dsn:
            conn_user = psycopg2.extensions.parse_dsn(self.dsn)
        else:
            conn_user = {}

        conn_args = conn_airflow | conn_user

        return conn_args

    def get_cur(self, conn: psycopg2.extensions.connection):
        """
        Создаёт cursor
        """
        cursor_name = self.cursor_name
        cursor_factory = self.cursor_factory
        withhold = self.cursor_withhold or False
        scrollable = self.cursor_scrollable

        if cursor_factory:
            cur = conn.cursor(
                name=cursor_name,
                cursor_factory=cursor_factory,
                withhold=withhold,
                scrollable=scrollable,
            )
        else:
            cur = conn.cursor(
                name=cursor_name,
                withhold=withhold,
                scrollable=scrollable,
            )

        return cur

    def __call__(self, context):
        self.render_template_fields(context)

        stack = context[self.stack_key]
        pg_cur = self.get_cur_with_stack(stack)
        share = context[self.context_key]
        share["cur"] = pg_cur


def pg_auth_airflow_conn(
    conn_id: str,
    dsn: Optional[str] = None,
    conn_factory: Optional[Any] = None,
    cursor_factory: Optional[Any] = None,
    cursor_name: Optional[str] = None,
    cursor_withhold: Optional[bool] = None,
    cursor_scrollable: Optional[bool] = None,
):
    """
    Использовать указанный Airflow connection для подключения к postgres
    postgres_conn_id - может быть указано в виде jinja шаблона

    Airflow connection парсится на словарь атрибутов:
    host     - host в psycopg2
    schema   - dbname в psycopg2
    login    - user в psycopg2
    password - password в psycopg2
    port     - port  в psycopg2
    extra    - сюда можно передать остальные аргументы подключения в виде json словаря. данный словарь объединится с первичными аргументами

    Из указанных атрибутов будет сформирована dsn строка подключения, например:
    {'password': 'secret', 'user': 'postgres', 'dbname': 'test'}
    'dbname=test user=postgres password=secret'

    {'host': 'example.com', 'user': 'someone', 'dbname': 'somedb', 'connect_timeout': '10'}
    "postgresql://someone@example.com/somedb?connect_timeout=10"

    Все dsn атрибуты можно посмотреть тут: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.ConnectionInfo

    pg_auth_conn_id и pg_auth_dsn можно использовать совместно
    если указаны оба варианта, атрибуты сливаются в единый словарь
    поверх pg_auth_conn_id накладываются атрибуты pg_auth_dsn и переопределяют их
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id=conn_id,
                dsn=dsn,
                connection_factory=conn_factory,
                cursor_factory=cursor_factory,
                cursor_name=cursor_name,
                cursor_scrollable=cursor_scrollable,
                cursor_withhold=cursor_withhold,
            )
        )

        return builder

    return wrapper


def pg_auth_dsn(
    dsn: str,
    conn_factory: Optional[Any] = None,
    cursor_factory: Optional[Any] = None,
    cursor_name: Optional[str] = None,
    cursor_withhold: Optional[bool] = None,
    cursor_scrollable: Optional[bool] = None,
):
    """
    Указать dsn строку подключения
    Все dsn атрибуты можно посмотреть тут: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.ConnectionInfo

    pg_auth_conn_id и pg_auth_dsn можно использовать совместно
    если указаны оба варианта, атрибуты сливаются в единный словарь
    поверх pg_auth_conn_id накладываются атрибуты pg_auth_dsn и переопределяют их
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                dsn=dsn,
                connection_factory=conn_factory,
                cursor_factory=cursor_factory,
                cursor_name=cursor_name,
                cursor_scrollable=cursor_scrollable,
                cursor_withhold=cursor_withhold,
            )
        )

        return builder

    return wrapper


class PostgresExecuteModule(PipeTask):
    """Выполняет sql запрос без выполнения commit"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql,
        params,
    ):
        super().__init__(context_key)
        self.set_template_fields(["sql", "params"])
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.sql = sql
        self.params = params

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur: psycopg2.extensions.cursor = context[self.context_key][self.cur_key]
        pg_cur.execute(self.sql, self.params)


def pg_execute(sql: str, params=None):
    """
    Выполнить sql запрос
    commit не выполняется, только execute
    Для выполнения коcommit можно использовать
        @pg_commit
        @pg_execute_and_commit
    sql запрос может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
            )
        )
        return builder

    return wrapper


class PostgresExecuteAndCommitModule(PostgresExecuteModule):
    """Выполняет sql запрос и выполняет commit"""

    def __call__(self, context):
        super().__call__(context)

        pg_cur: psycopg2.extensions.cursor = context[self.context_key][self.cur_key]
        pg_cur.connection.commit()


def pg_execute_and_commit(sql: str, params=None):
    """
    Выполнить запрос и выполнить commit
    sql запрос может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresExecuteAndCommitModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
            )
        )
        return builder

    return wrapper


class PostgresExecuteFileModule(PipeTask):
    """Выполняет sql запрос из переданного файла без выполнения commit"""

    def __init__(
        self, context_key: str, template_render: Callable, sql_file: str, params
    ):
        super().__init__(context_key)
        super().set_template_fields(["sql_file", "params"])
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.sql_file = sql_file
        self.params = params

    def __call__(self, context):
        self.render_template_fields(context)
        self.sql_file = os.path.expandvars(self.sql_file)

        sql = (
            Path(self.sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        )

        print(f"try rendering file: {self.sql_file}")
        sql = self.template_render(sql, context)
        print(f"rendering success: {self.sql_file}")

        pg_cur = context[self.context_key][self.cur_key]
        pg_cur.execute(sql, self.params)


def pg_execute_file(sql_file: str, params=None):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresExecuteFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
            )
        )
        return builder

    return wrapper


class PostgresExecuteFileAndCommitModule(PostgresExecuteFileModule):
    """Выполнить sql запрос и после этого commit"""

    def __call__(self, context):
        super().__call__(context)

        pg_cur = context[self.context_key][self.cur_key]
        pg_cur.connection.commit()


def pg_execute_file_and_commit(sql_file: str, params=None):
    """
    Выполнить запрос из указанного sql файла и выполнить commit
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresExecuteFileAndCommitModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
            )
        )
        return builder

    return wrapper


class PostgresFetchToStdoutModule(PipeTask):
    """Считывает курсор и принтует данные в stdout"""

    def __init__(
        self,
        context_key: str,
    ):
        super().__init__(context_key)

        self.cur_key = "cur"

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]
        try:
            for record in pg_cur:
                print(record)
        except psycopg2.ProgrammingError as e:
            print(f"---- {e} -----")


def pg_fetch_to_stdout():
    """
    Прочитать данные построчно и вывести их в stdout
    Данные выводятся с помощью print в том виде, в котором их принял драйвер psycopg
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(PostgresFetchToStdoutModule(builder.context_key))
        return builder

    return wrapper


class PostgresCommitModule(PipeTask):
    def __init__(
        self,
        context_key: str,
    ):
        super().__init__(context_key)

        self.cur_key = "cur"

    def __call__(self, context):
        pg_cur = context[self.context_key][self.cur_key]
        pg_cur.connection.commit()


def pg_commit():
    """
    Выполнить commit
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCommitModule(
                builder.context_key,
            )
        )
        return builder

    return wrapper


def pg_module(module: Callable[[Any], None]):
    """
    Добавляет кастомный модуль обработки в конвейр PostgresBuilder
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(module)
        return builder

    return wrapper


class PostgresCopyExpertModule(PipeTask):
    """Выполняет copy_expert в указанный handler, например sys.stdout
    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy
    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        handler: TextIO,
        params=None,
        size_limit: Optional[int] = None,
    ):
        super().__init__(context_key)
        super().set_template_fields(["sql", "params", "size_limit"])
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.sql = sql
        self.handler = handler
        self.params = params
        self.size_limit = size_limit

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]

        if self.params:
            self.sql = pg_cur.mogrify(self.sql, self.params)

        print(f"copy expert and handler type: {type(self.handler)}")
        if self.size_limit:
            pg_cur.copy_expert(self.sql, self.handler, self.size_limit)
        else:
            pg_cur.copy_expert(self.sql, self.handler)


def pg_copy_to_handle(
    sql: str,
    handler: TextIO,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy в указанный handle (open() от файла или stdout)
    используя переданный sql запрос (принтует результат выполнения запроса)
    sql запрос может содержать jinja шаблоны

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertModule(
                builder.context_key,
                builder.template_render,
                sql,
                handler,
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlToStdoutModule(PostgresCopyExpertModule):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        params=None,
        size_limit: Optional[int] = None,
    ):
        super().__init__(
            context_key, template_render, sql, sys.stdout, params, size_limit
        )


def pg_copy_to_stdout(
    sql: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy в stdout используя переданный sql запрос (принтует результат выполнения запроса)
    sql запрос может содержать jinja шаблоны

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlToStdoutModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlAndFileModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        data_file: str,
        mode: str,
        params=None,
        size_limit: Optional[int] = None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ["sql", "data_file", "mode", "params", "size_limit"]
        )
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.sql = sql
        self.data_file = data_file
        self.params = params
        self.size_limit = size_limit
        self.mode = mode

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]

        self.data_file = os.path.expandvars(self.data_file)

        if self.params:
            self.sql = pg_cur.mogrify(self.sql, self.params)

        file = Path(self.data_file).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"postgres copy use file (mode: {self.mode}): {file} ...")

        with open(file, mode=self.mode) as f:
            if self.size_limit:
                pg_cur.copy_expert(self.sql, f, self.size_limit)
            else:
                pg_cur.copy_expert(self.sql, f)

        print(f"postgres copy success")


def pg_copy_to_file(
    sql: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy в файл используя переданный sql запрос
    sql запрос может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlAndFileModule(
                builder.context_key,
                builder.template_render,
                sql,
                to_file,
                "wb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


def pg_copy_from_file(
    sql: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy из файла используя переданный sql запрос
    Файл будет прочитан как как поток байт и отправлен в postgres
    sql может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlAndFileModule(
                builder.context_key,
                builder.template_render,
                sql,
                from_file,
                "rb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlAndGZipModule(PostgresCopyExpertSqlAndFileModule):
    """
    Модуль аналогичен PostgresCopyExpertSqlToFileModule
    Однако сохраняет данные в GZip формате
    """

    def __call__(self, context):
        super().__call__(context)

        pg_cur = context[self.context_key][self.cur_key]

        self.data_file = os.path.expandvars(self.data_file)

        if self.params:
            self.sql = pg_cur.mogrify(self.sql, self.params)

        file = Path(self.data_file).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"postgres copy use file (mode: {self.mode}): {file} ...")

        with gzip.open(file, mode=self.mode) as f:
            if self.size_limit:
                pg_cur.copy_expert(self.sql, f, self.size_limit)
            else:
                pg_cur.copy_expert(self.sql, f)

        print(f"postgres copy success")


def pg_copy_to_gzip(
    sql: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy в файл (архивируя поток в формате gzip) используя переданный sql запрос
    sql запрос может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql,
                to_file,
                "wb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


def pg_copy_from_gzip(
    sql: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy из заархивированного файла (в формате gzip) используя переданный sql запрос
    Внутри архива gzip может быть любой фалй, он будет прчитан как поток байт и отправлен в postgres
    sql запрос может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql,
                from_file,
                "rb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlFileAndFileModule(PipeTask):
    """Выполняет copy_expert команду из указанного sql файла
    путь к sql файлу можно указать в виде jinja шаблона
    sql стейт в файле может содержать jinja шаблоны
    В запросе необходимо передать TO STDOUT обязательно (запрос не должен содержать FROM STDIN)
    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        data_file: str,
        mode: str,
        params=None,
        size_limit: Union[int, None] = None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ["sql_file", "data_file", "mode", "params", "size_limit"]
        )
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.sql_file = sql_file
        self.data_file = data_file
        self.mode = mode
        self.params = params
        self.size_limit = size_limit

    def _open(self, file, mode):
        return open(file, mode=mode)

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]

        self.data_file = os.path.expandvars(self.data_file)
        self.sql_file = os.path.expandvars(self.sql_file)

        sql = (
            Path(self.sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        )
        print(f"try rendering file: {self.sql_file}")
        sql = self.template_render(sql, context)
        print(f"rendering success: {self.sql_file}")

        if self.params:
            sql = pg_cur.mogrify(sql, self.params)

        file = Path(self.data_file).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)
        print(f"postgres copy use file (mode: {self.mode}): {file} ...")

        with self._open(file, self.mode) as f:
            if self.size_limit:
                pg_cur.copy_expert(sql, f, self.size_limit)
            else:
                pg_cur.copy_expert(sql, f)

        print(f"postgres copy success")


def pg_copy_to_file_use_sql_file(
    sql_file: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy в файл используя переданный sql file
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlFileAndFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                to_file,
                "wb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlFileAndGZipModule(PostgresCopyExpertSqlFileAndFileModule):
    """
    Модуль аналогичен PostgresCopyExpertSqlToFileModule
    Однако сохраняет данные в GZip формате
    """

    def _open(self, file, mode):
        return gzip.open(file, mode=mode)


def pg_copy_from_file_use_sql_file(
    sql_file: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy из файла используя переданный sql file
    Файл будет прочитан как как поток байт и отправлен в postgres
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlFileAndFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                from_file,
                "rb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


def pg_copy_to_gzip_use_sql_file(
    sql_file: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy в файл (архивируя поток в формате gzip) используя переданный sql file
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlFileAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                to_file,
                "wb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


def pg_copy_from_gzip_use_sql_file(
    sql_file: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
):
    """
    Выполняет copy из заархивированного файла (в формате gzip) используя переданный sql file
    Внутри архива gzip может быть любой фалй, он будет прчитан как поток байт и отправлен в postgres
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresCopyExpertSqlFileAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                from_file,
                "rb",
                params,
                size_limit,
            )
        )
        return builder

    return wrapper


class PostgresSaveToXComModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        name: str,
        func: Callable[[psycopg2.extensions.cursor, Any], Any],
    ):
        super().__init__(context_key)
        super().set_template_fields(["name"])
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.ti_key = "ti"
        self.name = name
        self.func = func

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]
        res = self.func(pg_cur, context)
        res = self.template_render(res, context)
        ti = context[self.ti_key]

        ti.xcom_push(key=self.name, value=res)


def pg_save_to_xcom(
    xcom_name: str,
    xcom_gen: Callable[[psycopg2.extensions.cursor, Any], Any],
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования
    Для сохранения информации в xcom нужно передать:
    - xcom_name - это имя xcom записи. Напомню, что по умолчанию имя в xcom используется "return_value".
        Необходимо использовать любое другое имя, отличное от этого для передачи касомного xcom между задачами
    - xcom_gen - это функция, которая будет использована для генерации значения.
    xcom_gen принимает 1 параметр: psycopg2.extensions.cursor - текущий postgres cursor
    xcom_gen должна возвращать то значение, которое можно сериализовать в xcom

    Например можно сохранить кол-во строк, которые вернул postgres:
    @pg_save_to_xcom("myxcom", lambda cur, context: {
        'target_row': pg_cur.rownumber,
        'source_row': 0,
        'error_row': 0,
        "query": cur.query,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresSaveToXComModule(
                builder.context_key,
                builder.template_render,
                xcom_name,
                xcom_gen,
            )
        )
        return builder

    return wrapper


class PostgresSaveToContextModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        name: str,
        func: Callable[[psycopg2.extensions.cursor, Any], Any],
    ):
        super().__init__(context_key)
        super().set_template_fields(["name"])
        super().set_template_render(template_render)

        self.cur_key = "cur"
        self.ti_key = "ti"
        self.name = name
        self.func = func

    def __call__(self, context):
        self.render_template_fields(context)
        pg_cur = context[self.context_key][self.cur_key]
        res = self.func(pg_cur, context)
        res = self.template_render(res, context)
        context[self.name] = res


def pg_save_to_context(
    context_name: str,
    context_gen: Callable[[psycopg2.extensions.cursor, Any], Any],
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Для сохранения информации в context нужно передать:
    - context_name - это имя context ключа.
    - context_gen - это функция, которая будет использована для генерации значения, которое будет добавлено в context
    context_gen принимает 1 параметра: context
    context_gen должна возвращать то значение, которое унжно сохранить в context

    Например можно сохранить кол-во строк, которые вернул postgres:
    @pg_save_to_context("my_context_key", lambda cur, context: {
        'target_row': "{{ params.target_row }}",
        'source_row': 0,
        'error_row': 0,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresSaveToContextModule(
                builder.context_key,
                builder.template_render,
                context_name,
                context_gen,
            )
        )
        return builder

    return wrapper


class PostgresInsertDictionaryModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        schema_name: str,
        table_name: str,
        payload: Dict,
    ):
        super().__init__(context_key)
        super().set_template_fields(["schema_name", "table_name", "payload"])
        super().set_template_render(template_render)

        self.context_key = context_key
        self.template_render = template_render
        self.schema_name = schema_name
        self.table_name = table_name
        self.payload = payload

    def _sql_parts(self, record: Dict[str, Any]):
        values = map(lambda x: "%s", record.keys())
        values = ", ".join(values)

        names = map(lambda x: psycopg2.sql.Identifier(x), record.keys())
        names = psycopg2.sql.SQL(", ").join(names)

        params = map(lambda x: x, record.values())
        params = tuple(params)

        return (names, values, params)

    def __call__(self, context):
        self.render_template_fields(context)
        pg_cur = context[self.context_key]["cur"]

        # необходимо для обработки dict
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        names_stmp, values_stmp, params = self._sql_parts(self.payload)
        names_stmp = names_stmp.as_string(pg_cur)
        stmp = (
            "insert into {schema_name}.{table_name} ("
            + names_stmp
            + ") values ("
            + values_stmp
            + ")"
        )
        stmp = psycopg2.sql.SQL(stmp)
        stmp = stmp.format(
            schema_name=psycopg2.sql.Identifier(self.schema_name),
            table_name=psycopg2.sql.Identifier(self.table_name),
        )
        stmp = stmp.as_string(pg_cur)

        pg_cur.execute(stmp, params)
        pg_cur.connection.commit()


def pg_insert_dict(
    schema_name: str,
    table_name: str,
    payload: Dict,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            PostgresInsertDictionaryModule(
                builder.context_key,
                builder.template_render,
                schema_name,
                table_name,
                payload,
            )
        )
        return builder

    return wrapper


def pg_register_uuid(oids=None):
    """
    Добавляет поддержку uuid типов данных
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_uuid(
                oids=oids,
                conn_or_curs=pg_cur,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_json(globally=False, loads=None, oid=None, array_oid=None, name="json"):
    """
    Добавляет поддержку json типов данных
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_json(
                conn_or_curs=pg_cur,
                globally=globally,
                loads=loads,
                oid=oid,
                array_oid=array_oid,
                name=name,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_inet(oid=None):
    """
    Добавляет поддержку inet типов данных
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_inet(
                oid=oid,
                conn_or_curs=pg_cur,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_hstore(globally=False, unicode=False, oid=None, array_oid=None):
    """
    Добавляет поддержку hstore хранения
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_hstore(
                conn_or_curs=pg_cur,
                globally=globally,
                unicode=unicode,
                oid=oid,
                array_oid=array_oid,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_default_json(globally=False, loads=None):
    """
    Добавляет поддержку hstore хранения
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_default_json(
                conn_or_curs=pg_cur,
                globally=globally,
                loads=loads,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_default_jsonb(globally=False, loads=None):
    """
    Добавляет поддержку hstore хранения
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_default_jsonb(
                conn_or_curs=pg_cur,
                globally=globally,
                loads=loads,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_composite(name, globally=False, factory=None):
    """
    Добавляет поддержку composite
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_composite(
                name=name,
                conn_or_curs=pg_cur,
                globally=globally,
                factory=factory,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_ipaddress():
    """
    Добавляет поддержку ipaddress
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_ipaddress(
                conn_or_curs=pg_cur,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


def pg_register_range(pgrange, pyrange, globally=False):
    """
    Добавляет поддержку ipaddress
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key]["cur"]
            psycopg2.extras.register_range(
                pgrange=pgrange,
                pyrange=pyrange,
                conn_or_curs=pg_cur,
                globally=globally,
            )

        builder.modules.append(mod)
        return builder

    return wrapper


class PostgresLoggerHandler(logging.Handler):
    def __init__(
        self,
        conn_id: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        conn_dsn: Optional[str] = None,
        extra_register: Optional[Callable[[psycopg2.extensions.cursor], None]] = None,
    ):
        self.conn_id = conn_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.conn_dsn = conn_dsn
        self.extra_register = extra_register

        super().__init__(level=0)

    def get_conn_and_cur(self, stack):
        conn = stack.enter_context(closing(self.get_conn()))
        cur = stack.enter_context(closing(self.get_cur(conn)))

        return conn, cur

    def get_airflow_conn(self, conn_id):
        try:
            return Connection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException as e:
            print(f"conn: {conn_id} not found. Please make sure it exists")
            raise e
        except Exception as e:
            raise e

    def get_conn_args_from_airflow(self, conn: Connection):
        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": conn.schema,
            "port": conn.port,
        }

        # любые опциональные параметры можно передать через блок extra в виде json
        if conn.extra_dejson:
            for name, val in conn.extra_dejson.items():
                conn_args[name] = val

        return conn_args

    def get_conn(self):
        if self.conn_id:
            conn_airflow = self.get_airflow_conn(self.conn_id)
            conn_airflow = self.get_conn_args_from_airflow(conn_airflow)
        else:
            conn_airflow = {}

        conn_args = conn_airflow

        if self.conn_dsn:
            conn_args |= psycopg2.extensions.parse_dsn(self.conn_dsn)

        conn = psycopg2.connect(**conn_args)

        return conn

    def get_cur(self, conn: psycopg2.extensions.connection):
        return conn.cursor()

    def emit(self, record: logging.LogRecord):
        schema_name, table_name = self._get_schema_and_table(record)

        with ExitStack() as stack:
            pg_conn, pg_cur = self.get_conn_and_cur(stack)

            psycopg2.extras.register_uuid(conn_or_curs=pg_cur)
            psycopg2.extras.register_json(conn_or_curs=pg_cur)
            psycopg2.extras.register_default_json(conn_or_curs=pg_cur)
            psycopg2.extensions.register_adapter(
                dict, psycopg2.extras.Json
            )  # необходимо для обработки dict

            if self.extra_register:
                self.extra_register(pg_cur)

            names_stmp, values_stmp, params = self._sql_parts(record)
            names_stmp = names_stmp.as_string(pg_cur)
            stmp = (
                "insert into {schema_name}.{table_name} ("
                + names_stmp
                + ") values ("
                + values_stmp
                + ")"
            )
            stmp = psycopg2.sql.SQL(stmp)
            stmp = stmp.format(
                schema_name=psycopg2.sql.Identifier(schema_name),
                table_name=psycopg2.sql.Identifier(table_name),
            )
            stmp = stmp.as_string(pg_cur)

            pg_cur.execute(stmp, params)
            pg_conn.commit()

    def _get_schema_and_table(self, record):
        schema_name = None

        if hasattr(record, "schema_name"):
            schema_name = record.schema_name
        elif self.schema_name:
            schema_name = self.schema_name
        else:
            raise ValueError(
                f'you forgot to pass the "scheme_name" for recording the log in the object: {type(self)}'
            )

        table_name = None

        if hasattr(record, "table_name"):
            table_name = record.table_name
        elif self.table_name:
            table_name = self.table_name
        else:
            raise ValueError(
                f'you forgot to pass the "table_name" for recording the log in the object: {type(self)}'
            )

        return (schema_name, table_name)

    def _sql_parts(self, record):
        values = map(lambda x: "%s", record.msg.keys())
        values = ", ".join(values)

        names = map(lambda x: psycopg2.sql.Identifier(x), record.msg.keys())
        names = psycopg2.sql.SQL(", ").join(names)

        params = map(lambda x: x, record.msg.values())
        params = tuple(params)

        return (names, values, params)


def get_postgres_logger(
    conn_id: str,
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    conn_dsn: Optional[str] = None,
    extra_register: Union[Callable[[psycopg2.extensions.cursor], None], None] = None,
    logger_name: Optional[str] = __name__,
):
    """
    Возвращает логгер который пишет dict в таблицу postgres
    умеет писать Json поля в виде Json

    пример использования:
    logger = get_postgres_logger('mylogger', 'airflow_connection_id', 'my_postgres_schema', 'my_postgres_table')
    logger.info({
        "log_id": uuid.uuid4(),
        "integration_id": 0,
        "process_name": "dag_name",
        "task_name": "my task name",
        "status": 1,
        "payload": {
            "target_row": pg_cur.rownumber,
            "source_row": 0,
            "error_row": 0,
        },
    })

    - logger_name - имя логера
    - conn_id - Airflow Connectio Id в котором содержится учётная запись для подключения к postgres
    - schema_name - схема по умолчанию, в которую буде писать logger
    - table_name - таблица по умолчанию, в которую буде писать logger
    - conn_dsn - дополнительные dsn параметры для postgres, например можно передать ApplicationName: "application_name=my_application_name"
    - extra_register - обработчик позволяющий сконфигурировать pg cursor перед выполнением insert. Может потребоваться, если объект логирования (Dict) содержит сложные типы данных, например hstore
    """
    pg_logger = logging.getLogger(logger_name)
    pg_logger.addHandler(
        PostgresLoggerHandler(
            conn_id, schema_name, table_name, conn_dsn, extra_register
        )
    )

    return pg_logger
