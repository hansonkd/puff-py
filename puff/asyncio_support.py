import asyncio
import functools
from threading import Thread
from . import set_async_type, ASYNCIO, rust_objects, wrap_async_asyncio


class AsyncioThread(Thread):
    def __init__(self, on_thread_start=None):
        self.event_queue = None
        self.loop = None
        self.on_thread_start = on_thread_start
        super().__init__()

    def run(self):
        set_async_type(ASYNCIO)
        if self.on_thread_start is not None:
            self.on_thread_start()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rust_objects.asyncio_loop = loop

        self.loop = loop
        loop.run_forever()

    def spawn(self, make_coroutine, args, kwargs, ret_func):
        async def task_coro():
            try:
                ret = await make_coroutine(*args, **kwargs)
                ret_func(ret, None)
            except Exception as e:
                ret_func(None, e)

        asyncio.run_coroutine_threadsafe(task_coro(), self.loop)

    def spawn_coro(self, coroutine, ret_func):
        async def task_coro():
            try:
                ret = await coroutine
                ret_func(ret, None)
            except Exception as e:
                ret_func(None, e)

        asyncio.run_coroutine_threadsafe(task_coro(), self.loop)


async def wrap_and_return(f, args, kwargs, ret):
    try:
        r = await f(*args, **kwargs)
        ret(r, None)
    except Exception as e:
        ret(None, e)


def start_event_loop(on_thread_start=None):
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass
    loop_thread = AsyncioThread(on_thread_start=on_thread_start)
    loop_thread.start()

    return loop_thread


def wrap_asgi(old_app):
    @functools.wraps(old_app)
    async def new_app(scope, py_receive, py_sender):
        async def receiver():
            return await wrap_async_asyncio(py_receive)

        async def sender(msg):
            return py_sender(msg)

        return await old_app(scope, receiver, sender)

    return new_app
