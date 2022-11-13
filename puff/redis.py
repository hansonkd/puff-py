from typing import Optional, Dict, List, Tuple, Any
from . import Bytelike, rust_objects, wrap_async


class RedisClient:
    def __init__(self, client=None, client_fn=None):
        self.redis = None
        self.client_fn = client_fn or rust_objects.global_redis_getter

    def client(self):
        rc = self.redis
        if rc is None:
            self.redis = rc = self.client_fn()
        return rc

    def get(self, key: Bytelike) -> Optional[bytes]:
        return wrap_async(lambda rr: self.client().get(rr, key))

    def set(self, key: Bytelike, value: Bytelike, nx=None, ex=None):
        return wrap_async(lambda rr: self.client().set(rr, key, value, ex, nx))

    def mset(self, values: Dict[Bytelike, Bytelike], nx=None):
        if isinstance(values, dict):
            values = values.items()
        return wrap_async(lambda rr: self.client().mset(rr, values, nx))

    def mget(self, keys: List[Bytelike]) -> List[bytes]:
        keys = [key for key in keys]
        return wrap_async(lambda rr: self.client().mget(rr, keys))

    def persist(self, key: Bytelike) -> bool:
        return wrap_async(lambda rr: self.client().persist(rr, key))

    def expire(self, key: Bytelike, seconds: int) -> bool:
        return wrap_async(lambda rr: self.client().expire(rr, key, seconds))

    def delete(self, key: Bytelike) -> bool:
        return wrap_async(lambda rr: self.client().delete(rr, key))

    def incr(self, key: Bytelike, delta: int) -> int:
        return wrap_async(lambda rr: self.client().incr(rr, key, delta))

    def decr(self, key: Bytelike, delta: int) -> int:
        return wrap_async(lambda rr: self.client().decr(rr, key, delta))

    def lpop(self, key: Bytelike, count: int = 1) -> Optional[bytes]:
        return wrap_async(lambda rr: self.client().lpop(rr, key, count))

    def rpop(self, key: Bytelike, count: int = 1) -> Optional[bytes]:
        return wrap_async(lambda rr: self.client().rpop(rr, key, count))

    def blpop(self, key: Bytelike, timeout: int) -> Optional[Tuple[bytes, bytes]]:
        return wrap_async(lambda rr: self.client().blpop(rr, key, timeout))

    def brpop(self, key: Bytelike, timeout: int) -> Optional[Tuple[bytes, bytes]]:
        return wrap_async(lambda rr: self.client().brpop(rr, key, timeout))

    def lpush(self, key: Bytelike, value: Bytelike) -> int:
        return wrap_async(lambda rr: self.client().lpush(rr, key, value))

    def rpush(self, key: Bytelike, value: Bytelike) -> int:
        return wrap_async(lambda rr: self.client().rpush(rr, key, value))

    def rpoplpush(self, key: Bytelike, destination: Bytelike) -> int:
        return wrap_async(lambda rr: self.client().rpoplpush(rr, key, destination))

    def command(self, command: List[Bytelike]) -> Any:
        return wrap_async(lambda rr: self.client().command(rr, command))

    def pipeline(self):
        return self

    def execute(self):
        return self


global_redis = RedisClient()


def named_client(name: str = "default"):
    return RedisClient(client_fn=lambda: rust_objects.global_redis_getter.by_name(name))
