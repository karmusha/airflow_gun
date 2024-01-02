"""
Позволяет выполнить http запрос, написав его в стиле Fluent API
Все декораторы объявлены с префиксом @http_
Цепочка декораторов должна:
 - начинаться с airflow декоратора с python_callable, например @task.python, @task.sensor
 - заканчиваться декоратором @http_run

Декорируемый метод выполняется последней в цепочке
Декорируемый метод должен быть объявлен с сигнатурой
    my_func(*args, **kwargs)
Первые два аргумента всегда request и response:
    - request: urllib.request.Request
    - response: http.client.HTTPResponse
Остальные аргументы зависят от контекста выполнения
и могут содержать в том числе xcom из предыдущего task

Декорируемый метод должен возвращать объект,
который может быть сериализован в xcom, например простой словарь:
    {
        'code': res.getcode(),
        'message': res.msg,
        'headers': res.getheaders(),
        'url': res.geturl(),
        'body': res.read1(),
    }

Можно передать xcom из предыдущего task следующим образом
    wait_job(run_mapping())
    - run_mapping   - первый task, который запускает загрузку на внешней системе
    - wait_job      - второй task, который принимает
        request, response и результат выполнения первого task (run_mapping)
Пример:
    # выполним http post запрос
    # сформируем url запроса используя jinja
    # в конце обработаем результат в функции run_mapping
    # args[0] - это request
    # args[1] - это response

    @task.python
    @http_post(
        "http://{{ params.host }}:{{ params.port }}/DataIntegrationService/modules/ms/v1/applications/{{ params.app }}/mappings/{{ params.mapping }}"
    )
    @http_run()
    def run_mapping(*args, **kwargs):
        return {
            "JobId": "my_job_id",
        }

    # запустим сенсор, который будет выполнять http запросы в течение 3600 секунд, каждые 10 секунд
    # сформируем url запроса используя jinja
    # в конце запроса проверим показатель и выйдем из сенсора (is_done=True)
    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule")
    @http_post(
        "http://{{ params.host }}:{{ params.port }}/DataIntegrationService/modules/ms/v1/applications/{{ params.app }}/mappings/{{ params.mapping }}"
    )
    @http_run()
    def wait_job(*args, **kwargs) -> PokeReturnValue:
        req = args[0]
        res = args[1]
        xcom = args[2]
        jobid = xcom["JobId"]
        return PokeReturnValue(is_done=True, xcom_value="xcom_value")

    wait_job(run_mapping())

"""

import sys
import json
import ssl
import gzip
import shutil

from typing import Dict, List, Optional
from collections.abc import Iterable
from pathlib import Path

from urllib.request import (
    Request,
    build_opener,
    HTTPHandler,
    BaseHandler,
    HTTPSHandler,
    HTTPError,
    HTTPDefaultErrorHandler,
    HTTPBasicAuthHandler,
    HTTPDigestAuthHandler,
)

from airflow.providers.gun.http.informatica_auth import InformaticaAuthHandler
from airflow.providers.gun.http.kerberos_auth import HTTPKerberosAuthHandler
from airflow.providers.gun.http.ntlm_auth import HTTPNtlmAuthHandler
from airflow.providers.gun.http.preemptive_basic_auth import PreemptiveBasicAuthHandler
from airflow.providers.gun.http.airflow_http_password_mgr import (
    AirflowHTTPConnectionPasswordMgr,
)

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder


__all__ = [
    "http_req",
    "http_get",
    "http_post",
    "http_put",
    "http_run",
    "http_headers",
    "http_body_dict",
    "http_body_file",
    "http_body_string",
    "http_body_bytes",
    "http_body_iterable",
    "http_auth_basic",
    "http_auth_preemptive_basic",
    "http_auth_digest",
    "http_auth_informatica",
    "http_auth_ntlm",
    "http_auth_kerberos",
    "http_auth_preemptive_basic_with_conn_id",
    "http_auth_basic_from_conn_id",
    "http_auth_digest_from_conn_id",
    "http_auth_informatica_from_conn_id",
    "http_auth_ntlm_from_conn_id",
    "http_auth_kerberos_from_conn_id",
    "http_auth_conn_id",
    "http_error_if_code",
    "http_error_supress",
    "http_retry_if_code",
    "http_response_body_to_gzip",
    "http_response_body_to_stdout",
]


def _flatten(code):
    """
    превращает вложенные списки в один
    [[1, 2, 4], 5, [34]] => [1, 2, 4, 5, 34]
    """
    match code:
        case int(code):
            return code
        case str(code):
            code = int(code)
            return code
        case code if isinstance(code, Iterable):
            for co in code:
                if isinstance(co, Iterable) and not isinstance(co, (str, bytes)):
                    for sub_x in _flatten(co):
                        yield sub_x
                else:
                    yield co

            return code
        case _:
            raise ValueError(
                f'{__name__} requires "code" be passed, example: 401 or [401,404,500], or range(400, 600)'
            )


class HttpReq(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        url,
        method,
        timeout,
        ssl_verify,
        debuglevel,
    ):
        super().__init__(context_key)
        super().set_template_fields(("url", "method", "timeout", "ssl_verify"))
        super().set_template_render(template_render)

        self.url = url
        self.method = method
        self.timeout = timeout
        self.ssl_verify = ssl_verify
        self.debuglevel = debuglevel
        self.request_key = "req"
        self.timeout_key = "timeout"
        self.modules_key = "modules"
        self.ssl_verify_key = "ssl_verify"
        self.debuglevel_key = "debuglevel"

    def __call__(self, context):
        self.render_template_fields(context)

        req = Request(
            url=self.url,
            method=self.method,
        )

        share = context[self.context_key]
        share[self.request_key] = req
        share[self.timeout_key] = self.timeout
        share[self.modules_key] = []
        share[self.ssl_verify_key] = self.ssl_verify
        share[self.debuglevel_key] = self.debuglevel


def http_req(
    url: str,
    method: str = "GET",
    timeout: Optional[int] = None,
    ssl_verify=False,
    debuglevel: int = 0,
):
    """
    Выполнить http запрос по указанному url
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpReq(
                builder.context_key,
                builder.template_render,
                url,
                method,
                timeout,
                ssl_verify,
                debuglevel,
            )
        )

        return builder

    return wrapper


def http_get(url: str, timeout: Optional[int] = None, ssl_verify=False, debuglevel=0):
    """
    Выполнить GET запрос по указанному URL
    """
    return http_req(url, "GET", timeout, ssl_verify, debuglevel)


def http_post(url: str, timeout: Optional[int] = None, ssl_verify=False, debuglevel=0):
    """
    Выполнить POST запрос по указанному URL
    """

    return http_req(url, "POST", timeout, ssl_verify, debuglevel)


def http_put(url: str, timeout: Optional[int] = None, ssl_verify=False, debuglevel=0):
    """
    Выполнить PUT запрос по указанному URL
    """

    return http_req(url, "PUT", timeout, ssl_verify, debuglevel)


def http_delete(
    url: str, timeout: Optional[int] = None, ssl_verify=False, debuglevel=0
):
    """
    Выполнить DELETE запрос по указанному URL
    """

    return http_req(url, "DELETE", timeout, ssl_verify, debuglevel)


class HttpRes(PipeTask):
    def __init__(
        self,
        context_key,
        stack_key,
        template_render,
    ):
        super().__init__(context_key)
        super().set_template_fields(())
        super().set_template_render(template_render)

        self.stack_key = stack_key
        self.request_key = "req"
        self.timeout_key = "timeout"
        self.modules_key = "modules"
        self.ssl_verify_key = "ssl_verify"
        self.debuglevel_key = "debuglevel"
        self.response_key = "res"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        stack = context[self.stack_key]
        req = share[self.request_key]
        timeout = share[self.timeout_key]
        modules: List = share[self.modules_key]
        debuglevel = share[self.debuglevel_key]
        ssl_verify = share[self.ssl_verify_key]

        if not ssl_verify:
            ssl._create_default_https_context = ssl._create_unverified_context

        modules.insert(0, HTTPSHandler(debuglevel))
        modules.insert(0, HTTPHandler(debuglevel))

        opener = build_opener(*modules)
        res = opener.open(fullurl=req, timeout=timeout)
        res = stack.enter_context(res)

        share[self.response_key] = res


def http_run():
    """
    Запустить выполнение http запроса
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpRes(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
            )
        )

        return builder

    return wrapper


class HttpHeadersFromDictModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        headers,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "headers",
            ]
        )
        super().set_template_render(template_render)

        self.headers: Dict[str, str] = headers
        self.request_key = "req"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        req: Request = share[self.request_key]

        for key, val in self.headers.items():
            req.add_header(key, val)


def http_headers(headers: dict):
    """
    Добавить дополнительные заголовки в http запрос, например Content-Type
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpHeadersFromDictModule(
                builder.context_key,
                builder.template_render,
                headers,
            )
        )
        return builder

    return wrapper


class HttpReqBodyFromDictModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        body: dict,
        encoding="utf-8",
        ensure_ascii=False,
        default=str,
        errors="backslashreplace",
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "body",
                "encoding",
                "ensure_ascii",
                "default",
                "errors",
            ]
        )
        super().set_template_render(template_render)

        self.body = body
        self.encoding = encoding
        self.ensure_ascii = ensure_ascii
        self.default = default
        self.errors = errors

        self.request_key = "req"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        req: Request = share[self.request_key]

        body = json.dumps(
            self.body,
            ensure_ascii=self.ensure_ascii,
            default=self.default,
        )
        body = body.encode(
            encoding=self.encoding,
            errors=self.errors,
        )

        req.data = body


def http_body_dict(
    body: dict,
    encoding="utf-8",
    ensure_ascii=False,
    default=str,
    errors="backslashreplace",
):
    """
    Добавить request body в запрос из переданного словаря
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpReqBodyFromDictModule(
                builder.context_key,
                builder.template_render,
                body,
                encoding,
                ensure_ascii,
                default,
                errors,
            )
        )
        return builder

    return wrapper


class HTTPRetryRequestIfCode(HTTPDefaultErrorHandler):
    """
    Повторяет запрос при колучении указанного кода/кодов
    можно передать:
    - 401 - одиночный код
    - [401,404,500] - набор кодов
    - range(400, 600) - диапазон кодов
    """

    def __init__(self, code, retry, body_print):
        self.retry = retry
        self.body_print = body_print

        if isinstance(code, Iterable):
            self.code = _flatten(code)
        else:
            self.code = [code]

        for code in self.code:
            setattr(self, f"http_error_{code}", self._default_handler)
            setattr(self, f"https_error_{code}", self._default_handler)

    def _default_handler(self, req, res, errcode, errmsg, headers):
        if self.body_print:
            body = res.read()
            print("----- body -----")
            print(body)

        if self.retry <= 0:
            raise HTTPError(
                url=res.geturl(),
                code=errcode,
                msg=errmsg,
                hdrs=headers,
                fp=res,
            )
        else:
            print(
                f"\n> retry module: There are {self.retry} attempts left, I am executing the request again\n"
            )
            self.retry -= 1
            return self.parent.open(req, timeout=req.timeout)


class HttpRetryModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        code,
        retry,
        body_print,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "code",
                "retry",
                "body_print",
            ]
        )
        super().set_template_render(template_render)

        self.code = code
        self.retry = retry
        self.body_print = body_print

        self.modules_key = "modules"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.modules_key]

        modules.append(
            HTTPRetryRequestIfCode(
                self.code,
                self.retry,
                self.body_print,
            )
        )


def http_retry_if_code(
    code: int | list | range,
    retry: int,
    body_print: bool = True,
):
    """
    Добавить дополнительные попытки в случае получения указанных http кодов
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpRetryModule(
                builder.context_key,
                builder.template_render,
                code,
                retry,
                body_print,
            )
        )
        return builder

    return wrapper


class HTTPErrorSuppressProcessor(HTTPDefaultErrorHandler):
    """
    для того, что бы не генерировалось исключение в случае http ошибок
    выносим решение об исключении на уровень бизнес использования
    """

    def http_error_default(self, req, fp, code, msg, hdrs):
        return fp


class HTTPErrorIfCode(HTTPErrorSuppressProcessor):
    """
    Выдаёт исключение только при получении указанного/указанных http кодов
    можно передать:
    - 401 - одиночный код
    - [401,404,500] - набор кодов
    - range(400, 600) - диапазон кодов
    """

    def __init__(self, code):
        if isinstance(code, Iterable):
            self.code = _flatten(code)
        else:
            self.code = [code]

    def http_error_default(self, req, fp, code, msg, hdrs):
        if code in self.code:
            return self._default_handler(req, fp, code, msg, hdrs)

        return fp

    def _default_handler(self, req, fp, code, msg, hdrs):
        raise HTTPError(
            url=fp.geturl(),
            code=code,
            msg=msg,
            hdrs=hdrs,
            fp=fp,
        )


class HttpErrorSupressModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.modules_key = "modules"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.modules_key]

        modules.append(HTTPErrorSuppressProcessor())


def http_error_supress():
    """
    Подавить HTTPError если код ответа от сервера не является 200
    * позволяет вынести обработку, в том числе, ошибочных ответом в to_xcom модуль
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpErrorSupressModule(
                builder.context_key,
                builder.template_render,
            )
        )
        return builder

    return wrapper


class HttpErrorIfCodeModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        code,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "code",
            ]
        )
        super().set_template_render(template_render)

        self.code = code

        self.modules_key = "modules"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.modules_key]

        modules.append(
            HTTPErrorIfCode(
                self.code,
            )
        )


def http_error_if_code(
    code: int | list | range,
):
    """
    Добавить дополнительные попытки в случае получения указанных http кодов
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpErrorIfCodeModule(
                builder.context_key,
                builder.template_render,
                code,
            )
        )
        return builder

    return wrapper


class HttpBodyFromFileModule(PipeTask):
    def __init__(
        self,
        context_key,
        stack_key,
        template_render,
        file,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "file",
            ]
        )
        super().set_template_render(template_render)
        self.stack_key = stack_key
        self.file = file

        self.request_key = "req"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        stack = context[self.stack_key]
        req: Request = share[self.request_key]

        body = Path(self.file)
        body = body.open(mode="rb")
        body = stack.enter_context(body)

        req.data = body


def http_body_file(
    file: str,
):
    """
    Сформировать request body из переданного файла
    Файл должен существовать на момент выполнения Task
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpBodyFromFileModule(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                file,
            )
        )
        return builder

    return wrapper


class HttpBodyFromStringModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        content: str,
        encoding: str = "utf-8",
        errors="backslashreplace",
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "content",
                "encoding",
                "errors",
            ]
        )
        super().set_template_render(template_render)

        self.content = content
        self.encoding = encoding
        self.errors = errors

        self.request_key = "req"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]

        req: Request = share[self.request_key]

        body = self.content.encode(
            encoding=self.encoding,
            errors=self.errors,
        )

        req.data = body


def http_body_string(
    content: str,
    encoding: str = "utf-8",
    errors="backslashreplace",
):
    """
    Сформировать request body из обычной строки текста.
    Строка декодится в набор байт согласно указанной кодировке
    В случае невозможности декодировать символ, сработает указанное правило в errors
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpBodyFromStringModule(
                builder.context_key,
                builder.template_render,
                content,
                encoding,
                errors,
            )
        )
        return builder

    return wrapper


class HttpBodyFromBytesModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        content,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.content = content

        self.request_key = "req"

    def __call__(self, context):
        share = context[self.context_key]

        req: Request = share[self.request_key]

        req.data = self.content


def http_body_bytes(
    content: bytes,
):
    """
    Сформировать request body из указанного набора байт
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpBodyFromBytesModule(
                builder.context_key,
                builder.template_render,
                content,
            )
        )
        return builder

    return wrapper


class HttpBodyFromIterableModule(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        content,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.content = content

        self.request_key = "req"

    def __call__(self, context):
        share = context[self.context_key]

        req: Request = share[self.request_key]

        req.data = self.content


def http_body_iterable(
    content: Iterable,
):
    """
    Сформировать request body из указанного итерируемого объекта
    Итератор должен возвращать набор bytes при каждой итерации
    Запрос с таким объектом выполняется в потоке отсылая каждый next по открытому socket
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpBodyFromIterableModule(
                builder.context_key,
                builder.template_render,
                content,
            )
        )
        return builder

    return wrapper


class HttpAddhandler(PipeTask):
    def __init__(
        self,
        context_key,
        template_render,
        *handler,
    ):
        super().__init__(context_key)
        super().set_template_fields([])
        super().set_template_render(template_render)

        self.handler = handler

        self.modules_key = "modules"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.modules_key]

        for m in self.handler:
            modules.append(m)


def http_handler(
    *handler: BaseHandler,
):
    """
    Добавить дополнительные кастомные модули обработки запросов
        - Модуль должен быть подклассом базового типа: urllib.request.BaseHandler
        - https://docs.python.org/3/library/urllib.request.html
        - модули добавляются в указанной последовательности
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                *handler,
            )
        )
        return builder

    return wrapper


class HTTPSaveResponseBodyToFileGzip(BaseHandler):
    """
    Сохраняет response body в файл
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def http_response(self, request, response):
        with gzip.open(Path(self.file_path), "wb") as f_out:
            shutil.copyfileobj(response, f_out)

        return response

    https_response = http_response


class HttpResBodyToGzipModule(PipeTask):
    def __init__(
        self,
        context_key,
        stack_key,
        template_render,
        file_path,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "file_path",
            ]
        )
        super().set_template_render(template_render)

        self.stack_key = stack_key
        self.file_path = file_path

        self.modules_key = "modules"

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.modules_key]

        modules.append(HTTPSaveResponseBodyToFileGzip(self.file_path))


def http_response_body_to_gzip(
    file_path: str,
):
    """
    Сохранить response body в файл "file_path"
    file_path мжно указать в виде jinja шаблона:
        {{ params.file_path }}
        {{ task_instance.xcom_pull(task_ids='prev_task_id', key='file_path') }}
    Помни что response body можно прочитать из сокета только один раз
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpResBodyToGzipModule(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                file_path,
            )
        )
        return builder

    return wrapper


class HTTPPrintResponseBodyToStdout(BaseHandler):
    """
    Принтует response body в stdout
    """

    def http_response(self, request, response):
        shutil.copyfileobj(response, sys.stdout.buffer)

        return response

    https_response = http_response


def http_response_body_to_stdout():
    """
    Притануть response body в stdout
    Помни что response body можно прочитать из сокета только один раз
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPPrintResponseBodyToStdout(),
            )
        )
        return builder

    return wrapper


def http_auth_basic(
    password_mgr,
):
    """
    Добавляет модуль basic авторизации
    password_mgr - менеджер паролей из которого будет взять username и password:
        https://docs.python.org/3/library/urllib.request.html#httppasswordmgr-objects
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPBasicAuthHandler(password_mgr),
            )
        )
        return builder

    return wrapper


def http_auth_preemptive_basic(password_mgr):
    """
    Добавляет модуль preemptive basic авторизации
    basic авторизация выполняется в два этапа:
        - формируется запрос без авторизации
        - в случае получения кода 401, формируется второй запрос с Authorization заголовком
    preemptive_basic - выполняется в один этап, заголовок Authorization
        отправляются в первом же запросе
    password_mgr - менеджер паролей из которого будет взять username и password:
        https://docs.python.org/3/library/urllib.request.html#httppasswordmgr-objects
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                PreemptiveBasicAuthHandler(password_mgr),
            )
        )
        return builder

    return wrapper


def http_auth_digest(password_mgr):
    """
    Добавляет модуль digest авторизации
    password_mgr - менеджер паролей из которого будет взять
        username и password:
            https://docs.python.org/3/library/urllib.request.html#httppasswordmgr-objects
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                HTTPDigestAuthHandler(password_mgr),
            )
        )
        return builder

    return wrapper


def http_auth_informatica(password_mgr):
    """
    Добавляет модуль informatica авторизации, которая необходима для rest api
    password_mgr - менеджер паролей из которого будет взять
        username и password:
            https://docs.python.org/3/library/urllib.request.html#httppasswordmgr-objects
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                InformaticaAuthHandler(password_mgr),
            )
        )
        return builder

    return wrapper


def http_auth_ntlm(password_mgr):
    """
    Добавляет модуль ntlm авторизации
    password_mgr - менеджер паролей из которого будет взять
        username и password:
            https://docs.python.org/3/library/urllib.request.html#httppasswordmgr-objects
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPNtlmAuthHandler(password_mgr),
            )
        )
        return builder

    return wrapper


def http_auth_kerberos(password_mgr):
    """
    Добавляет модуль kerberos авторизации
    password_mgr - менеджер паролей из которого будет взять
        username и password:
            https://docs.python.org/3/library/urllib.request.html#httppasswordmgr-objects
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddhandler(
                builder.context_key,
                builder.template_render,
                HTTPKerberosAuthHandler(password_mgr),
            )
        )
        return builder

    return wrapper


class HttpAddAuthConnIdHandler(PipeTask):
    def __init__(
        self,
        context_key,
        stack_key,
        template_render,
        http_conn_id,
        handler_gen,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "http_conn_id",
            ]
        )
        super().set_template_render(template_render)
        self.stack_key = stack_key
        self.modules_key = "modules"
        self.http_conn_id = http_conn_id
        self.handler_gen = handler_gen

    def __call__(self, context):
        self.render_template_fields(context)

        share = context[self.context_key]
        modules: List = share[self.modules_key]

        modules.append(
            self.handler_gen(self),
        )


def http_auth_preemptive_basic_with_conn_id(http_conn_id):
    """
    Добавляет модуль preemptive basic авторизации
    Учётка берется из укзаанного Airflow http_conn_id
    basic авторизация выполняется в два этапа:
        - формируется запрос без авторизации
        - в случае получения кода 401, формируется второй запрос с Authorization заголовком
    preemptive_basic - выполняется в один этап,
        заголовок Authorization отправляются в первом же запросе
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: PreemptiveBasicAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id)
                ),
            )
        )
        return builder

    return wrapper


def http_auth_basic_from_conn_id(http_conn_id: str):
    """
    Добавляет модуль basic авторизации
    Учётка берется из укзаанного Airflow http_conn_id
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: HTTPBasicAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id)
                ),
            )
        )
        return builder

    return wrapper


def http_auth_digest_from_conn_id(http_conn_id: str):
    """
    Добавляет модуль digest авторизации
    Учётка берется из укзаанного Airflow http_conn_id
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: HTTPDigestAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id)
                ),
            )
        )
        return builder

    return wrapper


def http_auth_informatica_from_conn_id(http_conn_id: str):
    """
    Добавляет модуль informatica авторизации, которая необходима для rest api
    Учётка берется из укзаанного Airflow http_conn_id
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: InformaticaAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id),
                ),
            )
        )
        return builder

    return wrapper


def http_auth_ntlm_from_conn_id(http_conn_id: str):
    """
    Добавляет модуль ntlm авторизации
    Учётка берется из укзаанного Airflow http_conn_id
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: HTTPNtlmAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id)
                ),
            )
        )
        return builder

    return wrapper


def http_auth_kerberos_from_conn_id(http_conn_id: str):
    """
    Добавляет модуль kerberos авторизации
    Учётка берется из укзаанного Airflow http_conn_id
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: HTTPKerberosAuthHandler(
                    AirflowHTTPConnectionPasswordMgr(b.http_conn_id)
                ),
            )
        )
        return builder

    return wrapper


def http_auth_conn_id(http_conn_id: str, schema: str):
    """
    Добавляет универсальный модуль авторизации, согласно переданной схеме
    Учётка берется из укзаанного Airflow http_conn_id
    Поддерживаемые схемы:
     - basic
     - preemptive_basic
     - digest
     - ntlm
     - informatica
    """

    def schema_verify(schema, password_mgr):
        match schema:
            case "basic":
                return HTTPBasicAuthHandler(password_mgr)
            case "preemptive_basic":
                return PreemptiveBasicAuthHandler(password_mgr)
            case "digest":
                return HTTPDigestAuthHandler(password_mgr)
            case "ntlm":
                return HTTPNtlmAuthHandler(password_mgr)
            case "kerberos":
                return HTTPKerberosAuthHandler(password_mgr)
            case "informatica":
                return InformaticaAuthHandler(password_mgr)
            case _:
                raise ValueError(f'unsupported authorization schema: "{schema}"')

    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            HttpAddAuthConnIdHandler(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                http_conn_id,
                lambda b: schema_verify(
                    schema, AirflowHTTPConnectionPasswordMgr(b.http_conn_id)
                ),
            )
        )
        return builder

    return wrapper
