import pendulum

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.decorators import task

from airflow.providers.gun.pipe import pipe
from airflow.providers.gun.ch import (
    ch_auth_airflow_conn,
    ch_send_data_from_csv,
    ch_send_query,
    ch_send_queries,
    ch_send_file_query,
    ch_send_file_queries,
    ch_send_external_tables,
    ch_recv_blocks_to_stdout,
    ch_save_to_csv,
    ch_save_to_gzip_csv,
)


with DAG(
    dag_id="tests__clickhouse_test",
    default_args={
        "owner": "solomatova.ms",
    },
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=30,
    tags=[],
    params={
        "conn_id": Param("clickhouse__spb99dkl-adqm01d"),
        "sql_file": Param("dags/tests/clickhouse_test/sql_file.sql"),
        "sql_file_multi_statements": Param(
            "dags/tests/clickhouse_test/sql_file_multi_statements.sql"
        ),
        "csv_file": Param("data/test_2.csv"),
        "gzip_csv_file": Param("data/test.gzip"),
    },
) as dag:
    dag.doc_md = __doc__

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query("select 1")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def select_one_row(_):
        pass

    select_one_row()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query("select 1")
    @ch_auth_airflow_conn(
        "{{ params.conn_id }}",
        settings={
            "client_name": "my_super_name",
        },
    )
    @pipe()
    def select_one_row_with_client_name(_):
        pass

    select_one_row_with_client_name()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query("select 1 union all select 2")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def select_two_row(_):
        pass

    select_two_row()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query(
        "select number as n, number+1 as m from numbers(0, 100) format csv",
        settings={
            "format_csv_delimiter": "|",
        },
    )
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def select_format_csv(_):
        pass

    select_format_csv()

    @task.python()
    @ch_send_queries(
        "select 1; select 1 one, 2 two; select version(); show databases; select number+1 as k from numbers(0, 10);"
    )
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def queries_comma_separated(_):
        pass

    queries_comma_separated()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query(
        "select number from numbers(0, 100000)",
        settings={
            "max_block_size": 65635,
        },
    )
    @ch_auth_airflow_conn(
        "{{ params.conn_id }}",
        settings={
            "compression": True,
        },
    )
    @pipe()
    def select_dohera_rows_with_compression(_):
        pass

    select_dohera_rows_with_compression()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_external_tables(
        [
            {
                "name": "ext",
                "structure": [("x", "Int32"), ("y", "Array(Int32)")],
                "data": [
                    {"x": 100, "y": [2, 4, 6, 8]},
                    {"x": 500, "y": [1, 3, 5, 7]},
                ],
            }
        ]
    )
    @ch_send_query("SELECT sum(x) FROM ext", end_query=False)
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def select_with_external_tables(_):
        pass

    select_with_external_tables()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query("show databases")
    @ch_recv_blocks_to_stdout()
    @ch_send_query("select version()")
    @ch_recv_blocks_to_stdout()
    @ch_send_query("select 1 as t")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def multiple_query(_):
        pass

    multiple_query()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_file_query("{{ params.sql_file }}")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def file_query(_):
        pass

    file_query()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_file_queries("{{ params.sql_file_multi_statements }}")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def file_queries_comma_separated(_):
        pass

    file_query()

    @task.python()
    @ch_save_to_csv("{{ params.csv_file }}")
    @ch_send_query("select number as number, number+1 mumbers from numbers(0, 10000)")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def save_to_csv(_):
        pass

    save_to_csv()

    @task.python()
    @ch_save_to_gzip_csv("{{ params.gzip_csv_file }}")
    @ch_send_query("show databases")
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def save_to_gzip_csv(_):
        pass

    save_to_gzip_csv()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query("SELECT name, comment FROM system.tables where name = 'temp1'")
    @ch_recv_blocks_to_stdout()
    @ch_send_query(
        "create temporary table temp1 (id UInt8) COMMENT 'The temporary table'"
    )
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def create_temp_table_and_select_description(_):
        pass

    create_temp_table_and_select_description()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_query(
        "select * from temp1",
        settings={
            "xcx": 3,
        },
    )
    @ch_recv_blocks_to_stdout()
    @ch_send_query("insert into temp1(id) select number from numbers(0, 100)")
    @ch_recv_blocks_to_stdout()
    @ch_send_query(
        "create temporary table temp1 (id UInt8) COMMENT 'The temporary table'",
        settings={
            "max_block_size": 3,
        },
    )
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def create_temp_table_and_insert_data(_):
        pass

    create_temp_table_and_insert_data()

    @task.python()
    @ch_recv_blocks_to_stdout()
    @ch_send_data_from_csv("dags/tests/clickhouse_test/send_data_from_csv_file.csv")
    @ch_send_query("insert into temp1 FORMAT CSV")
    @ch_recv_blocks_to_stdout()
    @ch_send_query(
        "create temporary table temp1 (id UInt8) COMMENT 'The temporary table'",
        settings={
            "max_block_size": 3,
        },
    )
    @ch_auth_airflow_conn("{{ params.conn_id }}")
    @pipe()
    def insert_data_from_csv_file(_):
        pass

    insert_data_from_csv_file()


if __name__ == "__main__":
    dag.test()
