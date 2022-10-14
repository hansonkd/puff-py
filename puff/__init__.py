import sys
import traceback
import contextvars
import dataclasses
from importlib import import_module
from typing import Any, Optional, List, Dict
from threading import Thread
import queue
import functools

from greenlet import greenlet


def blocking(func):
    if rust_objects.is_puff:

        @functools.wraps(func)
        def wrapper_blocking(*args, **kwargs):
            return spawn_blocking(func, *args, **kwargs).join()

        return wrapper_blocking
    else:
        return func


class RustObjects(object):
    """A class that holds functions and objects from Rust."""

    is_puff: bool = False
    global_state: Any

    def global_redis_getter(self):
        return None

    def global_postgres_getter(self):
        return None

    def global_pubsub_getter(self):
        return None

    def global_gql_getter(self):
        return None

    def read_file_bytes(self, rr: Any, fn: str) -> bytes:
        pass


rust_objects = RustObjects()

#  A global context var which holds information about the current executing thread.
parent_thread = contextvars.ContextVar("parent_thread")


@dataclasses.dataclass(frozen=True, slots=True)
class Task:
    args: list
    kwargs: dict
    ret_func: Any
    task_function: Any

    def process(self):
        new_greenlet = greenlet(self.task_function)
        new_greenlet.switch(self.args, self.kwargs, self.ret_func)


@dataclasses.dataclass(frozen=True, slots=True)
class Result:
    greenlet: Any

    def process(self):
        self.greenlet.switch()


@dataclasses.dataclass(frozen=True, slots=True)
class Kill:
    thread: Any

    def process(self):
        self.thread.kill_now()


@dataclasses.dataclass(frozen=True, slots=True)
class StartShutdown:
    thread: Any

    def process(self):
        self.thread.do_shutdown()


class MainThread(Thread):
    def __init__(self, event_queue, on_thread_start=None):
        self.event_queue = event_queue
        self.shutdown_started = False
        self.on_thread_start = on_thread_start
        self.main_greenlet = None
        self.event_loop_processor = None
        self.read_from_queue_processor = None
        self.greenlets = set()
        super().__init__()

    def run(self):
        if self.on_thread_start is not None:
            self.on_thread_start()
        self.main_greenlet = greenlet.getcurrent()
        self.event_loop_processor = greenlet(self.loop_commands)
        self.read_from_queue_processor = greenlet(self.read_from_queue)
        self.event_loop_processor.switch()
        while self.read_from_queue_processor.switch():
            pass

    def spawn(self, task_function, args, kwargs, ret_func):
        greenlet = self.new_greenlet()

        def wrapped_ret(val, e):
            self.complete_greenlet(greenlet)
            ret_func(val, e)

        self.spawn_local(task_function, args, kwargs, wrapped_ret)

    def spawn_local(self, task_function, args, kwargs, ret_func):
        task_function_wrapped = self.generate_spawner(task_function)
        task = Task(
            args=args,
            kwargs=kwargs,
            ret_func=ret_func,
            task_function=task_function_wrapped,
        )
        self.event_queue.put(task)

    def new_greenlet(self):
        greenlet = Greenlet(thread=self)
        self.greenlets.add(greenlet)
        return greenlet

    def complete_greenlet(self, greenlet):
        self.greenlets.remove(greenlet)

    def return_result(self, greenlet):
        result_event = Result(greenlet=greenlet)
        self.event_queue.put(result_event)

    def generate_spawner(self, func):
        def override_spawner(args, kwargs, ret_func):
            parent_thread.set(self)
            try:
                val = func(*args, **kwargs)
                ret_func(val, None)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, file=sys.stderr)
                traceback.print_exception(exc_value, file=sys.stderr)
                ret_func(None, e)

        return override_spawner

    def loop_commands(self):
        while True:
            event = self.read_next_event()
            event.process()

    def read_from_queue(self):
        while not self.has_shutdown():
            event = self.event_queue.get()
            self.event_loop_processor.switch(event)
        self.kill_now()

    def kill(self):
        self.event_queue.put(Kill(thread=self))

    def kill_now(self):
        self.main_greenlet.switch(False)

    def start_shutdown(self):
        self.event_queue.put(StartShutdown(thread=self))

    def do_shutdown(self):
        self.shutdown_started = True

    def has_shutdown(self):
        return self.shutdown_started and not self.greenlets

    def read_next_event(self):
        return self.main_greenlet.switch(True)


def start_event_loop(on_thread_start=None):
    q = queue.Queue()

    loop_thread = MainThread(q, on_thread_start=on_thread_start)
    loop_thread.start()

    return loop_thread


@dataclasses.dataclass
class Greenlet:
    thread: Any
    result: Any = None
    exception: Any = None
    finished: bool = False

    def join(self):
        while not self.finished:
            self.thread.event_loop_processor.switch()
        if self.exception:
            raise self.exception
        else:
            return self.result

    def set_result(self, result, exception):
        self.result = result
        self.exception = exception
        self.finished = True
        self.thread.complete_greenlet(self)

    def __hash__(self):
        return id(self)


def stob(s):
    if isinstance(s, str):
        return s.encode()
    return s


class RedisClient:
    def __init__(self, client=None):
        self.redis = None

    def client(self):
        if self.redis is None:
            self.redis = rust_objects.global_redis_getter()
        return self.redis

    def get(self, key: bytes) -> Optional[bytes]:
        return wrap_async(lambda rr: self.client().get(rr, stob(key)), join=True)

    def set(self, key: bytes, value: bytes, nx=None, ex=None):
        return wrap_async(
            lambda rr: self.client().set(rr, stob(key), stob(value), ex, nx), join=True
        )

    def mset(self, values: Dict[bytes, bytes], nx=None):
        vals = [(stob(k), stob(v)) for k, v in values.items()]
        return wrap_async(lambda rr: self.client().mset(rr, vals, nx), join=True)

    def mget(self, keys: List[bytes]) -> List[bytes]:
        keys = [stob(key) for key in keys]
        return wrap_async(lambda rr: self.client().mget(rr, keys), join=True)

    def persist(self, key: bytes) -> bool:
        return wrap_async(lambda rr: self.client().persist(rr, stob(key)), join=True)

    def expire(self, key: bytes, seconds: int) -> bool:
        return wrap_async(
            lambda rr: self.client().expire(rr, stob(key), seconds), join=True
        )

    def delete(self, key: bytes) -> bool:
        return wrap_async(lambda rr: self.client().delete(rr, stob(key)), join=True)

    def incr(self, key: bytes, delta: int) -> int:
        return wrap_async(
            lambda rr: self.client().incr(rr, stob(key), delta), join=True
        )

    def decr(self, key: bytes, delta: int) -> int:
        return wrap_async(
            lambda rr: self.client().decr(rr, stob(key), delta), join=True
        )

    def command(self, command: List[bytes]) -> Any:
        return wrap_async(lambda rr: self.client().command(rr, command), join=True)

    def pipeline(self):
        return self

    def execute(self):
        return self


class PubSubMessage:
    from_connection_id: str
    body: bytes
    text: Optional[str]


class PubSubConnection:
    def __init__(self, conn):
        self.conn = conn

    def who_am_i(self) -> str:
        return self.conn.who_am_i()

    def receive(self) -> Optional[PubSubMessage]:
        return wrap_async(lambda rr: self.conn.receive(rr), join=True)

    def subscribe(self, channel: str) -> bool:
        return wrap_async(lambda rr: self.conn.subscribe(rr, channel), join=True)

    def publish(self, channel: str, message: str) -> bool:
        return wrap_async(lambda rr: self.conn.publish(rr, channel, message), join=True)

    def publish_bytes(self, channel: str, message: bytes) -> bool:
        return wrap_async(
            lambda rr: self.conn.publish_bytes(rr, channel, message), join=True
        )


class PubSubClient:
    def __init__(self, client=None):
        self.pubsub = client

    def client(self):
        if self.pubsub is None:
            self.pubsub = rust_objects.global_pubsub_getter()
        return self.pubsub

    def new_connection_id(self) -> str:
        return self.client().new_connection_id()

    def publish_as(self, connection_id: str, channel: str, message: str) -> bool:
        return wrap_async(
            lambda rr: self.client().publish_as(rr, connection_id, channel, message),
            join=True,
        )

    def publish_bytes_as(
        self, connection_id: str, channel: str, message: bytes
    ) -> bool:
        return wrap_async(
            lambda rr: self.client().publish_bytes_as(
                rr, connection_id, channel, message
            ),
            join=True,
        )

    def connection(self) -> PubSubConnection:
        if self.pubsub is None:
            self.pubsub = rust_objects.global_pubsub_getter()

        return PubSubConnection(self.client().connection())

    def connection_with_id(self, connection_id: str) -> PubSubConnection:
        return PubSubConnection(self.client().connection_with_id(connection_id))


class GraphqlClient:
    def __init__(self, client=None):
        self.gql = client

    def client(self):
        if self.gql is None:
            self.gql = rust_objects.global_gql_getter()
        return self.gql

    def query(
        self, query: str, variables: Dict[str, Any], connection: Optional[Any] = None
    ) -> Any:
        return wrap_async(
            lambda rr: self.client().query(rr, query, variables, conn=connection),
            join=True,
        )


def wrap_async(f, join=True):
    this_greenlet = greenlet.getcurrent()
    thread = parent_thread.get()

    greenlet_obj = thread.new_greenlet()

    def return_result(r, e):
        greenlet_obj.set_result(r, e)
        thread.return_result(this_greenlet)

    f(return_result)
    if join:
        return greenlet_obj.join()
    else:
        return greenlet_obj


def spawn(f, *args, **kwargs):
    thread = parent_thread.get()
    this_greenlet = greenlet.getcurrent()
    greenlet_obj = thread.new_greenlet()

    def return_result(val, e):
        greenlet_obj.set_result(val, e)
        thread.return_result(this_greenlet)

    thread.spawn_local(f, args, kwargs, return_result, greenlet_obj=greenlet_obj)

    return greenlet_obj


def join_all(greenlets):
    if not greenlets:
        return []
    thread = greenlets[0].thread
    while not all(g.finished for g in greenlets):
        thread.event_loop_processor.switch()
    return [g.result for g in greenlets]


def join_iter(greenlets):
    if not greenlets:
        return None

    thread = greenlets[0].thread
    pending = set(greenlets)

    while pending:
        thread.event_loop_processor.switch()
        remove = set()
        for x in pending:
            if x.finished:
                yield x.result
                remove.add(x)
        pending = pending - remove


def global_redis():
    return RedisClient()


def global_pubsub():
    return PubSubClient()


def global_graphql():
    return GraphqlClient()


def global_state():
    return rust_objects.global_state


def spawn_blocking(f, *args, **kwargs):
    thread = parent_thread.get()
    child_thread = start_event_loop(on_thread_start=thread.on_thread_start)
    this_greenlet = greenlet.getcurrent()
    greenlet_obj = thread.new_greenlet()

    def return_result(val, e):
        greenlet_obj.set_result(val, e)
        thread.return_result(this_greenlet)
        child_thread.start_shutdown()

    child_thread.spawn(f, args, kwargs, return_result)

    return greenlet_obj


def spawn_blocking_from_rust(on_thread_start, f, args, kwargs, return_result):
    child_thread = start_event_loop(on_thread_start=on_thread_start)

    def wrap_return_result(val, e):
        return_result(val, e)
        child_thread.start_shutdown()

    child_thread.spawn(f, args, kwargs, wrap_return_result)


def cached_import(module_path, class_name):
    # Check whether module is loaded and fully initialized.
    if not ((module := sys.modules.get(module_path))):
        module = import_module(module_path)

    return getattr(module, class_name)


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    return cached_import(module_path, class_name)


def read_file_bytes(fn: str) -> bytes:
    return wrap_async(lambda rr: rust_objects.read_file_bytes(rr, fn), join=True)


context_id_var = contextvars.ContextVar("context_id")


def patch_asgi_ref_local():
    try:
        from asgiref import sync
    except ImportError:
        return None

    class ContextId:
        pass

    class SyncToAsync(sync.SyncToAsync):
        @staticmethod
        def get_current_task():
            """
            Implementation of asyncio.current_task()
            that returns None if there is no task.
            """
            if context_id := context_id_var.get(None):
                return context_id

            context_id = ContextId()
            context_id_var.set(context_id)
            return context_id

    sync.SyncToAsync = SyncToAsync


def patch_django():
    try:
        from django.views import static
    except ImportError:
        return None
    from puff.contrib.django.static import serve

    static.serve = serve


def patch_psycopg2():
    try:
        import psycopg2
    except ImportError:
        return None
    from puff import postgres

    for s in dir(postgres):
        if not s.startswith("__"):
            setattr(psycopg2, s, getattr(postgres, s))


def patch_libs():
    patch_asgi_ref_local()
    patch_django()
    patch_psycopg2()
