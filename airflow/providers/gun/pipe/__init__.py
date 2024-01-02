import random
import string
from typing import (
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Any,
    Tuple,
    Union,
)

from contextlib import ExitStack
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from airflow.utils.context import Context as AirflowContext
from airflow.utils.xcom import XCOM_RETURN_KEY

__all__ = [
    "pipe",
    "pipe_switch",
    "pipe_airflow_task",
    "pipe_airflow_callback",
    "pipe_airflow_callback_args",
    "PipeTaskBuilder",
]


class PipeTask:
    def __init__(self, context_key: str):
        self.template_fields: Sequence[str] = ()
        self.context_key = context_key
        self.template_render = lambda val, context: val

    def __call__(self, context):
        pass

    def set_template_fields(self, template_fields: Sequence[str]):
        self.template_fields = template_fields

    def set_template_render(self, template_render: Callable):
        self.template_render = template_render

    def render_template_fields(self, context):
        for name in self.template_fields:
            val = getattr(self, name)
            val = self.template_render(val, context)
            setattr(self, name, val)


class PipeTaskBuilder(PipeTask):
    def __init__(
        self,
        context_key: str,
        last_module: Callable,
    ):
        super().__init__(context_key)

        random_suffix = "".join(random.choices(string.ascii_uppercase, k=5))
        self.stack_key = f"{last_module.__name__}_stack_{random_suffix}"

        self.modules = []
        self.last_module = last_module

    def __call__(self, *args, **kwargs):
        context = args[0]

        for module in self.modules:
            if not context.get(module.context_key):
                context[module.context_key] = {}

            module(context)

        return self.last_module(*args, **kwargs)


def pipe_switch(context_key: str):
    def wrapper(pipe: PipeTaskBuilder):
        pipe.context_key = context_key
        return pipe

    return wrapper


class AirflowContextResolver:
    def __init__(self):
        pass

    def __call__(self, context: AirflowContext):
        raise NotImplementedError(f"{ __name__ } not implement __call__")


class XComPull(AirflowContextResolver):
    def __init__(
        self,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        key: str = XCOM_RETURN_KEY,
    ):
        self.task_ids = task_ids
        self.key = key
        self.task_instance_key = "task_instance"

    def __call__(self, context: AirflowContext) -> Any:
        ti: TaskInstance = context[self.task_instance_key]
        val = ti.xcom_pull(key=self.key, task_ids=self.task_ids)

        return val


class JinjaRender(AirflowContextResolver):
    def __init__(
        self,
        val: Any,
    ):
        self.val = val
        self.task_instance_key = "task"

    def __call__(self, context: AirflowContext) -> Any:
        task: TaskInstance = context[self.task_instance_key]
        jinja_env = task.get_template_env()
        val = task.render_template(
            self.val,
            context,
            jinja_env,
            set(),
        )
        return val


class PipeAirflowTaskBuilder(PipeTaskBuilder):
    def __init__(
        self,
        context_key: str,
        last_module: Callable,
    ):
        super().__init__(context_key, last_module)

        def render_template(val, context):
            if isinstance(val, XComPull):
                val = val(context)

            task = context["task"]
            jinja_env = task.get_template_env()
            val = task.render_template(
                val,
                context,
                jinja_env,
                set(),
            )
            return val

        self.set_template_render(render_template)
        self.last_module = last_module

        self.__name__ = last_module.__name__
        self.__doc__ = last_module.__doc__
        self.__annotations__ = last_module.__annotations__

    def __call__(self, *args, **kwargs):
        context = get_current_context()
        self.render_template_fields(context)

        with ExitStack() as stack:
            context[self.stack_key] = stack

            for module in self.modules:
                if not context.get(module.context_key):
                    context[module.context_key] = {}

                module(context)

            args = (context,) + args
            return self.last_module(*args)


def pipe_airflow_task(context_key: str = "pipe"):
    def wrapper(module):
        builder = PipeAirflowTaskBuilder(context_key, module)
        return builder

    return wrapper


class PipeAirflowCallbackBuilder(PipeAirflowTaskBuilder):
    def __init__(
        self,
        context_key: str,
        last_module: Callable,
    ):
        super().__init__(context_key, last_module)

    def __call__(self, context):
        random_suffix = "".join(random.choices(string.ascii_uppercase, k=5))
        self.stack_key = f"{self.last_module.__name__}_stack_{random_suffix}"

        self.render_template_fields(context)

        with ExitStack() as stack:
            context[self.stack_key] = stack

            for module in self.modules:
                if not context.get(module.context_key):
                    context[module.context_key] = {}

                module(context)

            return self.last_module(context)


class PipeAirflowCallbackArgsBuilder(PipeAirflowCallbackBuilder):
    def __init__(
        self,
        context_key: str,
        last_module: Callable,
    ):
        super().__init__(context_key, last_module)

    def __call__(self, *args, **kwargs):
        def wrapper_last_module(context):
            args_ = self.template_render(args, context)
            kwargs_ = self.template_render(kwargs, context)

            return self.last_module(context, *args_, **kwargs_)

        wrapper_last_module.__name__ = self.last_module.__name__
        wrapper_last_module.__doc__ = self.last_module.__doc__
        wrapper_last_module.__annotations__ = self.last_module.__annotations__

        wrapper = PipeAirflowCallbackBuilder(
            context_key=self.context_key,
            last_module=wrapper_last_module,
        )

        wrapper.modules = self.modules

        return wrapper


def pipe_airflow_callback(context_key: str = "pipe"):
    def wrapper(module):
        builder = PipeAirflowCallbackBuilder(context_key, module)
        return builder

    return wrapper


def pipe_airflow_callback_args(context_key: str = "pipe"):
    def wrapper(module):
        builder = PipeAirflowCallbackArgsBuilder(context_key, module)
        return builder

    return wrapper


class PipeDecoratorCollection:
    switch = staticmethod(pipe_switch)
    airflow_task = staticmethod(pipe_airflow_task)
    airflow_callback = staticmethod(pipe_airflow_callback_args)

    __call__: Any = airflow_task


pipe = PipeDecoratorCollection()


def jinja_render(decorated_function):
    def wrapper(*args, **context):
        res = decorated_function(*args)

        task = context["task"]
        jinja_env = task.get_template_env()
        res = task.render_template(
            res,
            context,
            jinja_env,
            set(),
        )

        return res

    wrapper.__name__ = decorated_function.__name__
    wrapper.__doc__ = decorated_function.__doc__
    wrapper.__annotations__ = decorated_function.__annotations__
    return wrapper
