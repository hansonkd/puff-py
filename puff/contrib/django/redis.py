from django.core.cache.backends.redis import RedisCacheClient, RedisCache

from puff import global_redis

redis_client = global_redis()


class PuffRedisCacheClient(RedisCacheClient):
    def get_client(self, key=None, *, write=False):
        return redis_client


class PuffRedisCache(RedisCache):
    def __init__(self, server, params):
        self._class = PuffRedisCacheClient
        self._options = params.get("OPTIONS", {})
