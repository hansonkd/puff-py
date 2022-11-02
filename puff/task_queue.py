from typing import Any, Optional
import time
import inspect

from . import wrap_async, rust_objects


DEFAULT_TIMEOUT = 30 * 1000
DEFAULT_KEEP_RESULTS_FOR = 5 * 60 * 1000


def current_milli_time():
    return round(time.time() * 1000)


class TaskQueue:
    def __init__(self, rust_tq=None):
        self.tq = rust_tq

    def client(self):
        tq = self.tq
        if self.tq is None:
            self.tq = tq = rust_objects.global_task_queue_getter()
        return tq

    def add_task(
        self,
        func_path: str,
        param: Any,
        unix_time_ms: int,
        timeout_ms: int,
        keep_results_for_ms: int,
        async_fn: bool,
        trigger: bool,
    ) -> bytes:
        return wrap_async(
            lambda r: self.client().add_task(
                r,
                func_path,
                param,
                unix_time_ms,
                timeout_ms,
                keep_results_for_ms,
                async_fn,
                trigger,
            ),
            join=True,
        )

    def task_result(self, task_id: bytes) -> Optional[Any]:
        return wrap_async(lambda r: self.client().task_result(r, task_id), join=True)

    def wait_for_task_result(
        self, task_id: bytes, poll_interval_ms: int = 100, timeout_ms: int = 10000
    ) -> Optional[Any]:
        return wrap_async(
            lambda r: self.client().wait_for_task_result(
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
        trigger=True,
    ) -> bytes:
        """
        Schedule a top-level importable function to be executed.

        Queues will compete to schedule the job as soon as it is first available. A job is available to be executed if
        scheduled_time_unix_ms < current_milli_time() at the time of the queue is looking and if all previous jobs have
        been completed. Once a job has been started, no other queues will execute the job until after `timeout_ms` have
        passed and no results are posted.

        trigger will cause one task queue that is waiting for new tasks to immediately look at the queue and pull a job.
        If trigger is False, the queue will trigger on the next loop, this will cause a slight delay of up to 1 second.
        """
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
            trigger,
        )


global_task_queue = TaskQueue()
