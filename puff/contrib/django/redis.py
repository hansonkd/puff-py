from django.core.cache.backends.redis import RedisCacheClient, RedisCache

from puff.redis import global_redis as redis_client


class PuffRedisCacheClient(RedisCacheClient):
    def get_client(self, key=None, *, write=False):
        return redis_client


class PuffRedisCache(RedisCache):
    def __init__(self, server, params):
        super().__init__(server, params)
        self._class = PuffRedisCacheClient
        self._options = params.get("OPTIONS", {})
