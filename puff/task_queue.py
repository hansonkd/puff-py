from typing import Any, Optional
import time
import inspect

from . import wrap_async, rust_objects


DEFAULT_TIMEOUT = 30 * 1000
DEFAULT_KEEP_RESULTS_FOR = 5 * 60 * 1000


def current_milli_time():
    return round(time.time() * 1000)


class TaskQueue:
    def __init__(self, rust_tq):
        self.tq = rust_tq

    def add_task(
        self,
        func_path: str,
        param: Any,
        unix_time_ms: int,
        timeout_ms: int,
        keep_results_for_ms: int,
        async_fn: bool,
    ) -> bytes:
        return wrap_async(
            lambda r: self.tq.add_task(
                r,
                func_path,
                param,
                unix_time_ms,
                timeout_ms,
                keep_results_for_ms,
                async_fn,
            ),
            join=True,
        )

    def task_result(self, task_id: bytes) -> Optional[Any]:
        return wrap_async(lambda r: self.tq.task_result(r, task_id), join=True)

    def wait_for_task_result(
        self, task_id: bytes, poll_interval_ms: int = 100, timeout_ms: int = 10000
    ) -> Optional[Any]:
        return wrap_async(
            lambda r: self.tq.wait_for_task_result(
                r, task_id, poll_interval_ms, timeout_ms
            ),
            join=True,
        )

    def schedule_function(
        self,
        func: Any,
        param: Any,
        scheduled_time_unix_ms=None,
        timeout_ms=DEFAULT_TIMEOUT,
        keep_results_for_ms=DEFAULT_KEEP_RESULTS_FOR,
    ) -> bytes:
        scheduled_time_unix_ms = scheduled_time_unix_ms or current_milli_time()
        mod_name = inspect.getmodule(func).__name__
        func_name = func.__name__
        async_fn = inspect.iscoroutinefunction(func)

        return self.add_task(
            f"{mod_name}.{func_name}",
            param,
            scheduled_time_unix_ms,
            timeout_ms,
            keep_results_for_ms,
            async_fn,
        )


def global_task_queue():
    return TaskQueue(rust_objects.global_task_queue_getter())
