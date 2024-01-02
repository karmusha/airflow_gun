from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from airflow.utils.context import Context as AirflowContext
from airflow.models.taskinstance import TaskInstance
from airflow.utils.xcom import XCOM_RETURN_KEY

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder

__all__ = [
    "back_module",
    "back_merge_if_error",
    "back_merge_if_not_error",
    "back_merge_error",
    "back_merge_with_xcom_dict",
    "back_merge_dict",
]


def back_module(module: Callable):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(module)
        return builder

    return wrapper


class MergeIfError(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        if_error: Callable[[Union[AirflowContext, Dict]], Dict],
        if_not_error: Union[Callable[[Union[AirflowContext, Dict]], Dict], None] = None,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.if_error = if_error
        self.if_not_error = if_not_error

    def __call__(self, context):
        ex = context.get("exception")
        back = context[self.context_key]

        if not ex:
            if self.if_not_error:
                res = self.if_not_error(context)
                back |= res
        else:
            res = self.if_error(context)
            back |= res


def back_merge_if_error(
    if_error: Callable[[Union[AirflowContext, Dict]], Dict],
    if_not_error: Union[Callable[[Union[AirflowContext, Dict]], Dict], None] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeIfError(
                builder.context_key,
                builder.template_render,
                if_error,
                if_not_error,
            )
        )
        return builder

    return wrapper


class MergeIfNotError(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        if_not_error: Callable[[Union[AirflowContext, Dict]], Dict],
        if_error: Union[Callable[[Union[AirflowContext, Dict]], Dict], None] = None,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.if_not_error = if_not_error
        self.if_error = if_error

    def __call__(self, context):
        ex = context.get("exception")
        back = context[self.context_key]

        if not ex:
            res = self.if_not_error(context)
            back |= res
        else:
            if self.if_error:
                res = self.if_error(context)
                back |= res


def back_merge_if_not_error(
    if_not_error: Callable[[Union[AirflowContext, Dict]], Dict],
    if_error: Union[Callable[[Union[AirflowContext, Dict]], Dict], None] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeIfNotError(
                builder.context_key,
                builder.template_render,
                if_not_error,
                if_error,
            )
        )
        return builder

    return wrapper


class MergeError(PipeTask):
    def __init__(self, context_key: str, template_render: Callable, save_to_key: str):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.save_to_key = save_to_key

    def __call__(self, context):
        back = context[self.context_key]
        back |= {
            self.save_to_key: context["exception"],
        }


def back_merge_error(save_to_key: str):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeError(
                builder.context_key,
                builder.template_render,
                save_to_key,
            )
        )
        return builder

    return wrapper


class MergeWithXComDict(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        key: str = XCOM_RETURN_KEY,
        check_exists: bool = False,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.task_ids = task_ids
        self.key = key
        self.check_exists = check_exists
        self.task_instance_key = "task_instance"

    def __call__(self, context):
        back = context[self.context_key]
        task_instance: TaskInstance = context[self.task_instance_key]
        val = task_instance.xcom_pull(task_ids=self.task_ids, key=self.key)

        if not val:
            if self.check_exists:
                raise ValueError(
                    f"failed to get the xcom value (key: {self.key}, task_ids: {self.task_ids})"
                )
        else:
            if not isinstance(val, Dict):
                raise ValueError(
                    f"xcom is not dictionary. type: {type(val)} (key: {self.key}, task_ids: {self.task_ids})"
                )

            back |= val


def back_merge_with_xcom_dict(
    task_ids: Optional[Union[str, Iterable[str]]] = None,
    key: str = XCOM_RETURN_KEY,
    check_exists: bool = False,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeWithXComDict(
                builder.context_key,
                builder.template_render,
                task_ids=task_ids,
                key=key,
                check_exists=check_exists,
            )
        )
        return builder

    return wrapper


class MergeWithXCom(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        save_to_key: str,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        key: str = XCOM_RETURN_KEY,
        check_exists: bool = False,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.save_to_key = save_to_key
        self.task_ids = task_ids
        self.key = key
        self.check_exists = check_exists
        self.task_instance_key = "task_instance"

    def __call__(self, context):
        back = context[self.context_key]
        task_instance: TaskInstance = context[self.task_instance_key]
        val = task_instance.xcom_pull(task_ids=self.task_ids, key=self.key)

        if not val:
            if self.check_exists:
                raise ValueError(
                    f"failed to get the xcom value (key: {self.key}, task_ids: {self.task_ids})"
                )
        else:
            back |= {self.save_to_key: val}


def back_merge_with_xcom(
    save_to_key: str,
    task_ids: Optional[Union[str, Iterable[str]]] = None,
    key: str = XCOM_RETURN_KEY,
    check_exists: bool = False,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeWithXCom(
                builder.context_key,
                builder.template_render,
                save_to_key=save_to_key,
                task_ids=task_ids,
                key=key,
                check_exists=check_exists,
            )
        )
        return builder

    return wrapper


class MergeWithDict(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        payload: Dict,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.payload = payload
        self.task_key = "task"

    def __call__(self, context):
        back = context[self.context_key]
        val = self.template_render(self.payload, context)

        back |= val


def back_merge_dict(
    payload: Dict,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeWithDict(
                builder.context_key,
                builder.template_render,
                payload,
            )
        )
        return builder

    return wrapper


class MergeWith(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        payload: Any,
        save_to_key: str,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.payload = payload
        self.save_to_key = save_to_key
        self.task_key = "task"

    def __call__(self, context):
        back = context[self.context_key]
        val = self.template_render(self.payload, context)

        back |= {self.save_to_key: val}


def back_merge(
    payload: Any,
    save_to_key: str,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeWith(
                builder.context_key,
                builder.template_render,
                payload,
                save_to_key,
            )
        )
        return builder

    return wrapper


class MergeWithContextDict(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        key: str,
        check_exists: bool = False,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.key = key
        self.check_exists = check_exists

    def __call__(self, context):
        back = context[self.context_key]
        val = context.get(self.key)

        if not val:
            if self.check_exists:
                raise ValueError(
                    f"failed to get the context value  with key: {self.key})"
                )
        else:
            if not isinstance(val, Dict):
                raise ValueError(
                    f"context value is not dictionary. type: {type(val)} (key: {self.key})"
                )

            back |= val


def back_merge_with_context_dict(
    key: str,
    check_exists: bool = False,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeWithContextDict(
                builder.context_key,
                builder.template_render,
                key=key,
                check_exists=check_exists,
            )
        )
        return builder

    return wrapper


class MergeWithContext(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        save_to_key: str,
        key: str,
        check_exists: bool = False,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.save_to_key = save_to_key
        self.key = key
        self.check_exists = check_exists

    def __call__(self, context):
        back = context[self.context_key]
        val = context.get(self.key)

        if not val:
            if self.check_exists:
                raise ValueError(
                    f"failed to get the context value  with key: {self.key})"
                )
        else:
            back |= {self.save_to_key: val}


def back_merge_with_context(
    save_to_key: str,
    key: str,
    check_exists: bool = False,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.modules.append(
            MergeWithContext(
                builder.context_key,
                builder.template_render,
                save_to_key=save_to_key,
                key=key,
                check_exists=check_exists,
            )
        )
        return builder

    return wrapper
