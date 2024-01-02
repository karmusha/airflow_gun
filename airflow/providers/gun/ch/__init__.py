import os
import io
import gzip
import csv
import logging

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)

from pathlib import Path

from clickhouse_driver.util.escape import escape_params
from clickhouse_driver.connection import Connection
from clickhouse_driver.protocol import ServerPacketTypes
from clickhouse_driver.block import RowOrientedBlock, ColumnOrientedBlock, BaseBlock
from clickhouse_driver.errors import UnexpectedPacketFromServerError

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection as AirflowConnection

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder

__all__ = [
    "ch_auth_airflow_conn",
    "ch_auth_dict",
    "ch_disconnect",
    "ch_send_query",
    "ch_send_queries",
    "ch_send_file_query",
    "ch_send_file_queries",
    "ch_send_external_tables",
    "ch_end_query",
    "ch_recv_blocks_to_stdout",
    "ch_recv_blocks",
    "ch_save_to_csv",
    "ch_save_to_gzip_csv",
    "ch_save_to_xcom",
    "ch_save_to_context",
]


class ClickhouseCursor:
    def __init__(self, *connection_args, **connection_kwargs):
        self.connection = Connection(*connection_args, **connection_kwargs)
        self.connection.context.settings = {}
        self.connection.context.client_settings = {
            "insert_block_size": 65656,
            "strings_as_bytes": False,
            "strings_encoding": "utf-8",
            "use_numpy": False,
            "opentelemetry_traceparent": None,
            "opentelemetry_tracestate": "",
            "quota_key": "",
            "input_format_null_as_default": False,
            "namedtuple_as_json": True,
        }

        self.logger = logging.getLogger(__name__)
        self._rowcount = -1

        super(ClickhouseCursor, self).__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def disconnect(self):
        self.connection.disconnect()

    def __iter__(self):
        return self

    def __next__(self) -> BaseBlock:
        try:
            while True:
                packet = self.connection.receive_packet()

                if packet is True:
                    continue

                if packet.type == ServerPacketTypes.END_OF_STREAM:
                    raise StopIteration()

                if packet.type == ServerPacketTypes.EXCEPTION:
                    raise packet.exception

                if packet.type == ServerPacketTypes.PROFILE_INFO:
                    self.logger.info(
                        f"blocks: {packet.profile_info.blocks}, bytes: {packet.profile_info.bytes}, rows: {packet.profile_info.rows}"
                    )

                if packet.type == ServerPacketTypes.PROGRESS:
                    if self._rowcount == -1:
                        self._rowcount = 0

                    self._rowcount += packet.progress.rows
                    self.logger.info(
                        f"total_rows: {packet.progress.total_rows}, bytes: {packet.progress.bytes}, rows: {packet.progress.rows}"
                    )

                if packet.type == ServerPacketTypes.DATA:
                    if not packet.block:
                        continue

                    if packet.block.num_rows == 0:
                        continue

                    return packet.block

                continue
        except StopIteration as e:
            raise e
        except (Exception, KeyboardInterrupt) as e:
            self.connection.disconnect()
            raise e

    def mogrify(self, query, params, context):
        if not isinstance(params, dict):
            raise ValueError("Parameters are expected in dict form")

        escaped = escape_params(params, context)
        return query % escaped

    @property
    def rowcount(self):
        return self._rowcount


class ClickhouseAuthAirflowConnection(PipeTask):
    def __init__(
        self,
        context_key: str,
        stack_key: str,
        template_render: Callable,
        conn_id: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "conn_id",
                "settings",
            )
        )
        super().set_template_render(template_render)

        self.stack_key = stack_key
        self.conn_id = conn_id
        self.settings = settings

    def get_airflow_conn(self, conn_id):
        try:
            return AirflowConnection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException as e:
            print(f"conn: {conn_id} not found. Please make sure it exists")
            raise e
        except Exception as e:
            raise e

    def get_conn_args_from_airflow(self, conn: AirflowConnection):
        """
        Преобразование Airflow connection в аргументы коннекшина для clickhouse_driver
        """
        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
            "port": conn.port,
        }

        # любые опциональные параметры можно передать через блок extra в виде json
        if conn.extra_dejson:
            for name, val in conn.extra_dejson.items():
                conn_args[name] = val

        return conn_args

    def get_cursor(self):
        if self.conn_id:
            conn_airflow = self.get_airflow_conn(self.conn_id)
            conn_airflow = self.get_conn_args_from_airflow(conn_airflow)
        else:
            conn_airflow = {}

        if self.settings and isinstance(self.settings, Dict):
            conn_user = self.settings
        else:
            conn_user = {}

        conn = conn_airflow | conn_user

        return ClickhouseCursor(conn.pop("host", "localhost"), **conn)

    def get_settings(self):
        """ """

        if self.conn_id:
            conn_airflow = self.get_airflow_conn(self.conn_id)
            conn_airflow = self.get_conn_args_from_airflow(conn_airflow)
        else:
            conn_airflow = {}

        if self.settings and isinstance(self.settings, Dict):
            conn_user = self.settings
        else:
            conn_user = {}

        return conn_airflow | conn_user

    def __call__(self, context):
        self.render_template_fields(context)

        stack = context[self.stack_key]
        cur = self.get_cursor()
        cur = stack.enter_context(cur)
        share = context[self.context_key]
        share["cur"] = cur


def ch_auth_airflow_conn(
    conn_id: str,
    settings: Optional[Dict[str, Any]] = None,
):
    """
    Represents connection between client and ClickHouse server.

    :param host: host with running ClickHouse server.
    :param port: port ClickHouse server is bound to.
                 Defaults to ``9000`` if connection is not secured and
                 to ``9440`` if connection is secured.
    :param database: database connect to. Defaults to ``'default'``.
    :param user: database user. Defaults to ``'default'``.
    :param password: user's password. Defaults to ``''`` (no password).
    :param client_name: this name will appear in server logs.
                        Defaults to ``'python-driver'``.
    :param connect_timeout: timeout for establishing connection.
                            Defaults to ``10`` seconds.
    :param send_receive_timeout: timeout for sending and receiving data.
                                 Defaults to ``300`` seconds.
    :param sync_request_timeout: timeout for server ping.
                                 Defaults to ``5`` seconds.
    :param compress_block_size: size of compressed block to send.
                                Defaults to ``1048576``.
    :param compression: specifies whether or not use compression.
                        Defaults to ``False``. Possible choices:

                            * ``True`` is equivalent to ``'lz4'``.
                            * ``'lz4'``.
                            * ``'lz4hc'`` high-compression variant of
                              ``'lz4'``.
                            * ``'zstd'``.

    :param secure: establish secure connection. Defaults to ``False``.
    :param verify: specifies whether a certificate is required and whether it
                   will be validated after connection.
                   Defaults to ``True``.
    :param ssl_version: see :func:`ssl.wrap_socket` docs.
    :param ca_certs: see :func:`ssl.wrap_socket` docs.
    :param ciphers: see :func:`ssl.wrap_socket` docs.
    :param keyfile: see :func:`ssl.wrap_socket` docs.
    :param certfile: see :func:`ssl.wrap_socket` docs.
    :param server_hostname: Hostname to use in SSL Wrapper construction.
                            Defaults to `None` which will send the passed
                            host param during SSL initialization. This param
                            may be used when connecting over an SSH tunnel
                            to correctly identify the desired server via SNI.
    :param alt_hosts: list of alternative hosts for connection.
                      Example: alt_hosts=host1:port1,host2:port2.
    :param settings_is_important: ``False`` means unknown settings will be
                                  ignored, ``True`` means that the query will
                                  fail with UNKNOWN_SETTING error.
                                  Defaults to ``False``.
    :param tcp_keepalive: enables `TCP keepalive <https://tldp.org/HOWTO/
                          TCP-Keepalive-HOWTO/overview.html>`_ on established
                          connection. If is set to ``True``` system keepalive
                          settings are used. You can also specify custom
                          keepalive setting with tuple:
                          ``(idle_time_sec, interval_sec, probes)``.
                          Defaults to ``False``.
    :param client_revision: can be used for client version downgrading.
                          Defaults to ``None``.
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id=conn_id,
                settings=settings,
            )
        )

        return builder

    return wrapper


def ch_auth_dict(settings: Dict):
    """
    Авторизоваться с помощью набора указанных параметров
    Доступные параметры аналогичны методу: ch_auth_airflow_conn_use
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                settings=settings,
            )
        )

        return builder

    return wrapper


class ClickhouseDisconnect(PipeTask):
    """ """

    def __init__(self, context_key: str):
        super().__init__(context_key)

    def __call__(self, context):
        cur = context[self.context_key]["cur"]
        cur.connection.disconnect()


def ch_disconnect():
    """ """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseDisconnect(
                builder.context_key,
            )
        )

        return builder

    return wrapper


class ClickhouseSendQuery(PipeTask):
    """
    Отправляет sql запрос в clickhouse
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        query: str,
        params,
        settings,
        query_id,
        end_query: bool,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "query",
                "query_id",
                "params",
                "settings",
                "end_query",
            )
        )
        super().set_template_render(template_render)

        self.query = query
        self.params = params
        self.query_id = query_id
        self.settings = settings
        self.end_query = end_query

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key]["cur"]
        query = self.query
        params = self.params
        query_id = self.query_id
        settings = self.settings
        end_query = self.end_query

        if params:
            query = cur.mogrify(query, params, cur.connection.context)

        if settings:
            cur.connection.context.settings |= settings

        cur.connection.send_query(query, query_id, params)

        if end_query:
            cur.connection.send_data(ColumnOrientedBlock())


def ch_send_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    end_query=True,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSendQuery(
                builder.context_key,
                builder.template_render,
                query,
                params,
                settings,
                query_id,
                end_query,
            )
        )

        return builder

    return wrapper


class ClickhouseSendQueryMultiStatementsModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        query: str,
        params,
        settings,
        query_id,
        sep,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "query",
                "query_id",
                "params",
                "settings",
                "sep",
            )
        )
        super().set_template_render(template_render)

        self.params = params
        self.query_id = query_id
        self.query: str = query
        self.settings = settings
        self.sep: str = sep

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key]["cur"]
        query = self.query
        settings = self.settings
        params = self.params
        query_id = self.query_id

        if params:
            query = cur.mogrify(query, params, cur.connection.context)

        if settings:
            cur.connection.context.settings |= settings

        for query_part in query.split(self.sep):
            if not query_part:
                continue

            print(query_part)
            cur.connection.send_query(query_part, query_id, params)
            cur.connection.send_data(ColumnOrientedBlock())

            first = True
            for block in cur:
                if first:
                    first = False
                    print(block.columns_with_types)

                for row in block.get_rows():
                    print(row)


def ch_send_queries(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    sql_file - может содержать несколько запросов, разделенных через ;
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSendQueryMultiStatementsModule(
                builder.context_key,
                builder.template_render,
                query,
                params,
                settings,
                query_id,
                sep=";",
            )
        )

        return builder

    return wrapper


class ClickhouseSendQueryFileModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params,
        settings,
        query_id,
        end_query: bool,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql_file",
                "params",
                "settings",
                "query_id",
            )
        )
        super().set_template_render(template_render)

        self.sql_file = sql_file
        self.params = params
        self.settings = settings
        self.query_id = query_id
        self.end_query = end_query
        self.query: str = ""

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key]["cur"]
        settings = self.settings
        params = self.params
        query_id = self.query_id
        end_query = self.end_query

        self.sql_file = os.path.expandvars(self.sql_file)

        print(f"try rendering file: {self.sql_file}")
        query = (
            Path(self.sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        )
        query = self.template_render(query, context)
        print(f"rendering success: {self.sql_file}")

        if params:
            query = cur.mogrify(query, params, cur.connection.context)

        if settings:
            cur.connection.context.settings |= settings

        print(query)

        cur.connection.send_query(query, query_id, params)

        if end_query:
            cur.connection.send_data(ColumnOrientedBlock())


def ch_send_file_query(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    end_query=True,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSendQueryFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                settings,
                query_id,
                end_query,
            )
        )

        return builder

    return wrapper


class ClickhouseSendQueryFileMultiStatementsModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params,
        settings,
        query_id,
        sep: str,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql_file",
                "params",
                "settings",
                "query_id",
                "sep",
            )
        )
        super().set_template_render(template_render)

        self.sql_file = sql_file
        self.params = params
        self.settings = settings
        self.query_id = query_id
        self.sep = sep

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key]["cur"]
        params = self.params
        settings = self.settings
        query_id = self.query_id

        self.sql_file = os.path.expandvars(self.sql_file)
        query = (
            Path(self.sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        )
        query = self.template_render(query, context)

        if params:
            query = cur.mogrify(query, params, cur.connection.context)

        if settings:
            cur.connection.context.settings |= settings

        for query_part in query.split(self.sep):
            if not query_part:
                continue

            print(query_part)
            cur.connection.send_query(query_part, query_id, params)
            cur.connection.send_data(ColumnOrientedBlock())

            first = True
            for block in cur:
                if first:
                    first = False
                    print(block.columns_with_types)

                for row in block.get_rows():
                    print(row)


def ch_send_file_queries(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    sql_file - может содержать несколько запросов, разделенных через ;
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSendQueryFileMultiStatementsModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                settings,
                query_id,
                sep=";",
            )
        )

        return builder

    return wrapper


class ClickhouseSendExternalTablesModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        external_tables,
        types_check: bool,
        end_query: bool,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.external_tables = external_tables
        self.types_check = types_check
        self.end_query = end_query

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key]["cur"]
        cur.connection.send_external_tables(self.external_tables, self.types_check)

        if self.end_query:
            cur.connection.send_data(RowOrientedBlock())


def ch_send_external_tables(
    external_tables: List[Dict[str, Any]],
    types_check: bool = False,
    end_query=True,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSendExternalTablesModule(
                builder.context_key,
                builder.template_render,
                external_tables,
                types_check,
                end_query,
            )
        )

        return builder

    return wrapper


class ClickhouseEndQueryModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

    def __call__(self, context):
        self.render_template_fields(context)
        cur = context[self.context_key]["cur"]
        cur.connection.send_data(RowOrientedBlock())


def ch_end_query():
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseEndQueryModule(
                builder.context_key,
                builder.template_render,
            )
        )

        return builder

    return wrapper


class ClickhouseResultRowToStdoutModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

    def __call__(self, context):
        self.render_template_fields(context)
        cur = context[self.context_key]["cur"]
        first = True
        for block in cur:
            if first:
                first = False
                print(block.columns_with_types)

            for row in block.get_rows():
                print(row)


def ch_recv_blocks_to_stdout():
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseResultRowToStdoutModule(
                builder.context_key,
                builder.template_render,
            )
        )

        return builder

    return wrapper


class ClickhouseReceiveAllBlocksModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        block_processing: Callable,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.block_processing = block_processing

    def __call__(self, context):
        self.render_template_fields(context)
        cur = context[self.context_key]["cur"]

        for block in cur:
            self.block_processing(block, context)


def ch_recv_blocks(block_processing: Callable):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseReceiveAllBlocksModule(
                builder.context_key,
                builder.template_render,
                block_processing,
            )
        )

        return builder

    return wrapper


class ClickhouseSaveToCsvModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        file_path: str,
        delimiter: str,
        quotechar: Optional[str],
        escapechar: Optional[str],
        doublequote: bool,
        skipinitialspace: bool,
        lineterminator: str,
        quoting: int,
        strict: bool,
        encoding: str,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "file_path",
                "delimiter",
                "quotechar",
                "escapechar",
                "doublequote",
                "skipinitialspace",
                "quoting",
                "strict",
                "encoding",
            )
        )
        super().set_template_render(template_render)

        self.file_path = file_path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.lineterminator = lineterminator
        self.quoting = quoting
        self.strict = strict
        self.encoding = encoding

    def __call__(self, context):
        self.render_template_fields(context)

        cur = context[self.context_key]["cur"]

        self.file_path = os.path.expandvars(self.file_path)
        file = Path(self.file_path).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"clickhouse result copy to: {file} ...")

        with open(file, mode="w", newline="") as f:
            wrt = csv.writer(
                f,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                doublequote=self.doublequote,
                skipinitialspace=self.skipinitialspace,
                lineterminator=self.lineterminator,
                quoting=self.quoting,
                strict=self.strict,
            )

            first = True
            for block in cur:
                if first:
                    first = False
                    csv_header = map(lambda x: x[0], block.columns_with_types)
                    wrt.writerow(csv_header)

                for row in block.get_rows():
                    wrt.writerow(row)


def ch_save_to_csv(
    to_file: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    quoting: int = csv.QUOTE_MINIMAL,
    strict: bool = False,
    encoding: str = "utf-8",
):
    """ """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSaveToCsvModule(
                builder.context_key,
                builder.template_render,
                to_file,
                delimiter,
                quotechar,
                escapechar,
                doublequote,
                skipinitialspace,
                lineterminator,
                quoting,
                strict,
                encoding,
            )
        )

        return builder

    return wrapper


class ClickhouseSaveToGZipCsvModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        file_path: str,
        delimiter: str,
        quotechar: Optional[str],
        escapechar: Optional[str],
        doublequote: bool,
        skipinitialspace: bool,
        lineterminator: str,
        quoting: int,
        strict: bool,
        encoding: str,
        compresslevel: int,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "file_path",
                "delimiter",
                "quotechar",
                "escapechar",
                "doublequote",
                "skipinitialspace",
                "quoting",
                "strict",
                "encoding",
                "compresslevel",
            )
        )
        super().set_template_render(template_render)

        self.file_path = file_path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.lineterminator = lineterminator
        self.quoting = quoting
        self.strict = strict
        self.encoding = encoding
        self.compresslevel = compresslevel

    def __call__(self, context):
        self.render_template_fields(context)

        cur = context[self.context_key]["cur"]

        self.file_path = os.path.expandvars(self.file_path)
        file = Path(self.file_path).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"clickhouse result copy to: {file} ...")

        with gzip.open(file, mode="wb", compresslevel=self.compresslevel) as f:
            with io.TextIOWrapper(f, encoding=self.encoding, newline="") as wrapper:
                wrt = csv.writer(
                    wrapper,
                    delimiter=self.delimiter,
                    quotechar=self.quotechar,
                    escapechar=self.escapechar,
                    doublequote=self.doublequote,
                    skipinitialspace=self.skipinitialspace,
                    lineterminator=self.lineterminator,
                    quoting=self.quoting,
                    strict=self.strict,
                )

                first = True
                for block in cur:
                    if first:
                        first = False
                        csv_header = map(lambda x: x[0], block.columns_with_types)
                        wrt.writerow(csv_header)

                    for row in block.get_rows():
                        wrt.writerow(row)


def ch_save_to_gzip_csv(
    to_file: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    quoting: int = csv.QUOTE_MINIMAL,
    strict: bool = False,
    encoding: str = "utf-8",
    compresslevel: int = 9,
):
    """ """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSaveToGZipCsvModule(
                builder.context_key,
                builder.template_render,
                to_file,
                delimiter,
                quotechar,
                escapechar,
                doublequote,
                skipinitialspace,
                lineterminator,
                quoting,
                strict,
                encoding,
                compresslevel,
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
        func: Callable[[ClickhouseCursor, Any], Any],
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


def ch_save_to_xcom(
    xcom_name: str,
    xcom_gen: Callable[[ClickhouseCursor, Any], Any],
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования
    Для сохранения информации в xcom нужно передать:
    - xcom_name - это имя xcom записи. Напомню, что по умолчанию имя в xcom используется "return_value".
        Необходимо использовать любое другое имя, отличное от этого для передачи касомного xcom между задачами
    - xcom_gen - это функция, которая будет использована для генерации значения.
    xcom_gen принимает 1 параметр: ClickhouseCursor - текущий clickhouse cursor
    xcom_gen должна возвращать то значение, которое можно сериализовать в xcom

    @ch_save_to_xcom("myxcom", lambda cur, context: {
        'source_row': 0,
        'error_row': 0,
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


class ClickhouseSaveToContextModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        name: str,
        func: Callable[[ClickhouseCursor, Any], Any],
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


def ch_save_to_context(
    context_name: str,
    context_gen: Callable[[ClickhouseCursor, Any], Any],
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Для сохранения информации в context нужно передать:
    - context_name - это имя context ключа.
    - context_gen - это функция, которая будет использована для генерации значения, которое будет добавлено в context
    context_gen принимает 1 параметра: context
    context_gen должна возвращать то значение, которое унжно сохранить в context

    @ch_save_to_context("my_context_key", lambda cur, context: {
        'target_row': "{{ params.target_row }}",
        'source_row': 0,
        'error_row': 0,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                context_name,
                context_gen,
            )
        )
        return builder

    return wrapper


class RowOrientedTextBlock(BaseBlock):
    dict_row_types = (dict,)
    tuple_row_types = (list, tuple)
    supported_row_types = dict_row_types + tuple_row_types

    def normalize(self, data):
        if not data:
            return []

        # # Guessing about whole data format by first row.
        # first_row = data[0]

        # if self.types_check:
        #     self._check_row_type(first_row)

        # if isinstance(first_row, dict):
        #     self._mutate_dicts_to_rows(data)
        # else:
        #     self._check_rows(data)

        return data

    @property
    def num_columns(self):
        if self.columns_with_types is not None:
            return len(self.columns_with_types)

        return len(self.data[0]) if self.num_rows else 0

    @property
    def num_rows(self):
        return len(self.data)

    def get_columns(self):
        return self.transposed()

    def get_rows(self):
        return self.data

    def get_column_by_index(self, index):
        return [row[index] for row in self.data]

    def _mutate_dicts_to_rows(self, data):
        check_row_type = False
        if self.types_check:
            check_row_type = self._check_dict_row_type

        return self._pure_mutate_dicts_to_rows(
            data,
            self.columns_with_types,
            check_row_type,
        )

    def _pure_mutate_dicts_to_rows(
        self,
        data,
        columns_with_types,
        check_row_type,
    ):
        columns_with_cwt = []
        for name, type_ in columns_with_types:
            cwt = None
            if type_.startswith("Nested"):
                inner_spec = get_inner_spec("Nested", type_)
                cwt = get_inner_columns_with_types(inner_spec)
            columns_with_cwt.append((name, cwt))

        for i, row in enumerate(data):
            if check_row_type:
                check_row_type(row)

            new_data = []
            for name, cwt in columns_with_cwt:
                if cwt is None:
                    new_data.append(row[name])
                else:
                    new_data.append(
                        self._pure_mutate_dicts_to_rows(row[name], cwt, check_row_type)
                    )
            data[i] = new_data
        # return for recursion
        return data

    def _check_rows(self, data):
        expected_row_len = len(self.columns_with_types)

        got = len(data[0])
        if expected_row_len != got:
            msg = "Expected {} columns, got {}".format(expected_row_len, got)
            raise ValueError(msg)

        if self.types_check:
            check_row_type = self._check_tuple_row_type
            for row in data:
                check_row_type(row)

    def _check_row_type(self, row):
        if not isinstance(row, self.supported_row_types):
            raise TypeError(
                "Unsupported row type: {}. dict, list or tuple is expected.".format(
                    type(row)
                )
            )

    def _check_tuple_row_type(self, row):
        if not isinstance(row, self.tuple_row_types):
            raise TypeError(
                "Unsupported row type: {}. list or tuple is expected.".format(type(row))
            )

    def _check_dict_row_type(self, row):
        if not isinstance(row, self.dict_row_types):
            raise TypeError(
                "Unsupported row type: {}. dict is expected.".format(type(row))
            )


class ClickhouseSendDataFromCsvModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        file_path: str,
        delimiter: str,
        quotechar: Optional[str],
        escapechar: Optional[str],
        doublequote: bool,
        skipinitialspace: bool,
        lineterminator: str,
        quoting: int,
        strict: bool,
        encoding: str,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "file_path",
                "delimiter",
                "quotechar",
                "escapechar",
                "doublequote",
                "skipinitialspace",
                "quoting",
                "strict",
                "encoding",
            )
        )
        super().set_template_render(template_render)

        self.file_path = file_path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.lineterminator = lineterminator
        self.quoting = quoting
        self.strict = strict
        self.encoding = encoding

    def receive_sample_block(self, conn: Connection):
        while True:
            packet = conn.receive_packet()

            if packet.type == ServerPacketTypes.DATA:
                return packet.block

            elif packet.type == ServerPacketTypes.EXCEPTION:
                raise packet.exception

            elif packet.type == ServerPacketTypes.LOG:
                pass
                # log_block(packet.block)

            elif packet.type == ServerPacketTypes.TABLE_COLUMNS:
                pass

            else:
                message = conn.unexpected_packet_message(
                    "Data, Exception, Log or TableColumns", packet.type
                )
                raise UnexpectedPacketFromServerError(message)

    def __call__(self, context):
        self.render_template_fields(context)

        cur = context[self.context_key]["cur"]

        self.file_path = os.path.expandvars(self.file_path)
        file = Path(self.file_path).absolute()

        print(f"clickhouse read csv: {file} ...")

        with open(file, mode="r", encoding=self.encoding) as f:
            sample_block = self.receive_sample_block(cur.connection)
            if sample_block:
                for row in f.readlines(3):
                    block = RowOrientedTextBlock(
                        sample_block.columns_with_types, row, types_check=False
                    )

                    cur.connection.send_data(block)


def ch_send_data_from_csv(
    from_file: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    quoting: int = csv.QUOTE_MINIMAL,
    strict: bool = False,
    encoding: str = "utf-8",
):
    """ """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            ClickhouseSendDataFromCsvModule(
                builder.context_key,
                builder.template_render,
                from_file,
                delimiter,
                quotechar,
                escapechar,
                doublequote,
                skipinitialspace,
                lineterminator,
                quoting,
                strict,
                encoding,
            )
        )

        return builder

    return wrapper
