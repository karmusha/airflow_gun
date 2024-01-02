import json
import gzip
import pendulum

from pathlib import Path

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.decorators import task

from airflow.providers.gun.pipe import pipe
from airflow.providers.gun.http import (
    http_body_bytes,
    http_body_dict,
    http_body_file,
    http_body_iterable,
    http_body_string,
    http_headers,
    http_req,
    http_get,
    http_post,
    http_run,
    http_response_body_to_stdout,
    http_retry_if_code,
    http_error_supress,
    http_error_if_code,
    http_handler,
    http_response_body_to_gzip,
)

from urllib.request import (
    Request,
    BaseHandler,
)


with DAG(
    dag_id="tests__http_test",
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
        "get_url": Param("https://httpbin.org/get"),
        "post_url": Param("https://httpbin.org/post"),
        "header_value_3": "header value",
        "body_param_4": "Body Params",
        "hello_world_file": Param("dags/tests/http_test/hello world.txt"),
        "hello_world_string": Param("hello world"),
        "http_save_response_to_gzip": Param("dags/tests/http_test/hello_world.gzip"),
    },
) as dag:
    dag.doc_md = __doc__

    @task.python()
    @http_run()
    @http_req("{{ params.get_url }}")
    @pipe.airflow_task("pipe")
    def http_1(context):
        res = context["pipe"]["res"]
        for r in res:
            print(r)

    http_1()

    @task.python()
    @http_run()
    @http_post("{{ params.post_url }}")
    @pipe.airflow_task("pipe")
    def http_2(context):
        res = context["pipe"]["res"]
        for r in res:
            print(r)

    http_2()

    @task.python()
    @http_run()
    @http_headers(
        {
            "My-Header": "{{ params.header_value_3 }}",
        }
    )
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_3(context):
        res = context["pipe"]["res"]
        res = res.read1()
        res = json.loads(res)
        res = res["headers"]
        res = res["My-Header"]
        print(res)
        assert res == "header value"

    http_3()

    @task.python()
    @http_run()
    @http_body_dict(
        {
            "my_params": "{{ params.body_param_4 }}",
        }
    )
    @http_headers(
        {
            "Content-Type": "application/json",
        }
    )
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_4(context):
        res = context["pipe"]["res"]
        res = res.read1()
        res = json.loads(res)
        res = res["json"]
        res = res["my_params"]
        print(res)
        assert res == "Body Params"

    http_4()

    @task.python()
    @http_run()
    @http_retry_if_code([400, 405], 10)
    @http_post("https://google.com", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_5(context):
        pass

    http_5()

    @task.python()
    @http_run()
    @http_error_supress()
    @http_post("https://google.com", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_6(context):
        res = context["pipe"]["res"]
        assert res.code == 405

    http_6()

    @task.python()
    @http_run()
    @http_error_if_code(
        [range(400, 405), 500]
    )  # 405 не включена в диапазон, ошибки не будет
    @http_post("https://google.com", debuglevel=1)  # post http://google.com выдает 405
    @pipe.airflow_task("pipe")
    def http_7(context):
        res = context["pipe"]["res"]
        assert res.code == 405

    http_7()

    @task.python()
    @http_run()
    @http_headers(
        {
            "Content-Type": "text/plain",
        }
    )
    @http_body_file("{{ params.hello_world_file }}")
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_send_transfer_encoding(context):
        res = context["pipe"]["res"]
        res = res.read1()
        res = json.loads(res)

        assert res["data"] == "hello world"

    http_send_transfer_encoding()

    @task.python()
    @http_run()
    @http_headers(
        {
            "Content-Type": "text/plain",
        }
    )
    @http_body_string("{{ params.hello_world_string }}")
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_send_string_encoding(context):
        res = context["pipe"]["res"]
        res = res.read1()
        res = json.loads(res)

        assert res["data"] == "hello world"

    http_send_string_encoding()

    @task.python()
    @http_run()
    @http_headers(
        {
            "Content-Type": "text/plain",
        }
    )
    @http_body_bytes(b"hello world")
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_send_bytes(context):
        res = context["pipe"]["res"]
        res = res.read1()
        res = json.loads(res)

        assert res["data"] == "hello world"

    http_send_bytes()

    def iterable_generator():
        yield b"1"
        yield b"2"
        yield b"3"
        yield b"4"
        yield b"5"
        yield b"6"

    @task.python()
    @http_run()
    @http_headers(
        {
            "Content-Type": "text/plain",
        }
    )
    @http_body_iterable(iterable_generator())
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_send_iterable_transfer_encoding(context):
        res = context["pipe"]["res"]
        res = res.read1()
        res = json.loads(res)

        assert res["data"] == "123456"

    http_send_iterable_transfer_encoding()

    class HTTP405ErrorSupressorHandler(BaseHandler):
        def http_error_405(self, req, fp, code, msg, hdrs):
            return fp

    @task.python()
    @http_run()
    @http_handler(HTTP405ErrorSupressorHandler())
    @http_post("https://google.com", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_custom_module(context):
        res = context["pipe"]["res"]

        assert res.code == 405

    http_custom_module()

    @task.python()
    @http_run()
    @http_response_body_to_gzip("{{ params.http_save_response_to_gzip }}")
    @http_headers(
        {
            "Content-Type": "text/plain",
        }
    )
    @http_body_bytes(b"hello world")
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_save_response_to_gzip(context, saved_file):
        with gzip.open(saved_file, mode="r") as f:
            res = f.read()
            res = json.loads(res)
            assert res["data"] == "hello world"

    http_save_response_to_gzip("{{ params.http_save_response_to_gzip }}")

    @task.python()
    @http_run()
    @http_response_body_to_stdout()
    @http_headers(
        {
            "Content-Type": "text/plain",
        }
    )
    @http_body_bytes(b"hello world")
    @http_post("{{ params.post_url }}", debuglevel=1)
    @pipe.airflow_task("pipe")
    def http_save_response_to_stdout(context):
        pass

    http_save_response_to_stdout()

if __name__ == "__main__":
    dag.test()
