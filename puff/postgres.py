import contextvars
import datetime
import functools
import time
import sys

from puff import wrap_async, rust_objects


threadsafety = 3
apilevel = "2.0"
paramstyle = "format"

"""Isolation level values."""
ISOLATION_LEVEL_AUTOCOMMIT = 0
ISOLATION_LEVEL_READ_UNCOMMITTED = 4
ISOLATION_LEVEL_READ_COMMITTED = 1
ISOLATION_LEVEL_REPEATABLE_READ = 2
ISOLATION_LEVEL_SERIALIZABLE = 3
ISOLATION_LEVEL_DEFAULT = None


@functools.total_ordering
class TypeObject(object):
    def __init__(self, *value_names):
        self.value_names = value_names
        self.values = value_names

    def __eq__(self, other):
        return other in self.values

    def __lt__(self, other):
        return self != other and other < self.values

    def __repr__(self):
        return "TypeObject" + str(self.value_names)


def _binary(string):
    if isinstance(string, str):
        return string.encode("utf-8")
    return bytes(string)


STRING = TypeObject("TEXT", "VARCHAR")
"""The type codes of TEXT result columns compare equal to this constant."""

BINARY = TypeObject("BYTEA")
"""The type codes of BLOB result columns compare equal to this constant."""

NUMBER = TypeObject("INT2", "INT4", "INT8", "FLOAT4", "FLOAT8")
"""The type codes of numeric result columns compare equal to this constant."""

DATETIME = TypeObject("TIMESTAMPZ", "TIMESTAMP")
"""The type codes of datetime result columns compare equal to this constant."""

DATE = TypeObject("DATE")
"""The type codes of date result columns compare equal to this constant."""

TIME = TypeObject("TIME")
"""The type codes of date result columns compare equal to this constant."""


ROWID = STRING

# comdb2 doesn't support Date or Time, so I'm not defining them.
Datetime = datetime.datetime
Binary = _binary
Timestamp = Datetime
Date = datetime.date
Time = datetime.time

DatetimeFromTicks = Datetime.fromtimestamp
TimestampFromTicks = Timestamp.fromtimestamp
DateFromTicks = Date.fromtimestamp


def TimeFromTicks(ticks):
    return time.gmtime(round(ticks))


class PostgresCursor:
    def __init__(self, cursor, connection):
        self.cursor = cursor
        self.last_query = None
        self.connection = connection
        self._description = None

    @property
    def rowcount(self):
        return wrap_async(lambda r: self.cursor.do_get_rowcount(r))

    @property
    def description(self):
        desc = self._description
        if desc is None:
            self._description = desc = wrap_async(lambda r: self.cursor.description(r))
        return desc

    @property
    def arraysize(self):
        return self.cursor.arraysize

    @arraysize.setter
    def arraysize(self, val):
        self.cursor.arraysize = val

    def setinputsizes(self, *args, **kwargs):
        pass

    def setoutputsize(self, *args, **kwargs):
        pass

    def execute(self, q, params=None):
        self.last_query = q.encode("utf8")
        self._description = None
        ix = 1
        params = list(params) if params is not None else None
        while "%s" in q:
            q = q.replace("%s", f"${ix}", 1)
            ix += 1
        ret = wrap_async(lambda r: self.cursor.execute(r, q, params))
        return ret

    def executemany(self, q, seq_of_params=None):
        self._description = None
        ix = 1
        while "%s" in q:
            q = q.replace("%s", f"${ix}", 1)
            ix += 1
        ret = wrap_async(lambda r: self.cursor.executemany(r, q, seq_of_params))
        return ret

    def fetchone(self):
        return wrap_async(lambda r: self.cursor.fetchone(r))

    def fetchmany(self, rowcount=None):
        return wrap_async(lambda r: self.cursor.fetchmany(r, rowcount))

    def fetchall(self):
        return wrap_async(lambda r: self.cursor.fetchall(r))

    def close(self):
        return self.cursor.close()

    def __del__(self):
        return self.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.fetchone()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    @property
    def query(self):
        return self.last_query


def get_client(dbname):
    if dbname is None:
        return rust_objects.global_postgres_getter()
    return rust_objects.global_postgres_getter.by_name(dbname)


class PostgresConnection:
    isolation_level = ISOLATION_LEVEL_DEFAULT
    server_version = 140000

    def __init__(self, client=None, autocommit=False, dbname=None):
        self._autocommit = autocommit
        self.postgres_client = client or get_client(dbname)

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @property
    def Warning(self):
        return sys.modules[__name__].Warning

    @property
    def Error(self):
        return sys.modules[__name__].Error

    @property
    def InterfaceError(self):
        return sys.modules[__name__].InterfaceError

    @property
    def DatabaseError(self):
        return sys.modules[__name__].DatabaseError

    @property
    def OperationalError(self):
        return sys.modules[__name__].OperationalError

    @property
    def IntegrityError(self):
        return sys.modules[__name__].IntegrityError

    @property
    def InternalError(self):
        return sys.modules[__name__].InternalError

    @property
    def ProgrammingError(self):
        return sys.modules[__name__].ProgrammingError

    @property
    def NotSupportedError(self):
        return sys.modules[__name__].NotSupportedError

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = value
        wrap_async(lambda rr: self.postgres_client.set_auto_commit(rr, value))

    def set_client_encoding(self, encoding, *args, **kwargs):
        if encoding != "UTF8":
            raise Exception("Only UTF8 Postgres encoding supported.")

    def get_parameter_status(self, parameter, *args, **kwargs):
        with self.cursor() as cursor:
            cursor.execute("SELECT current_setting($1)", [parameter])
            return cursor.fetchone()

    def set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def cursor(self, *args, **kwargs) -> PostgresCursor:
        return PostgresCursor(self.postgres_client.cursor(), self)

    def close(self):
        self.postgres_client.close()

    def commit(self):
        return wrap_async(lambda r: self.postgres_client.commit(r))

    def rollback(self):
        return wrap_async(lambda r: self.postgres_client.rollback(r))


connection_override = contextvars.ContextVar("connection_override")


def set_connection_override(connection):
    connection_override.set(connection)


def connect(*parameters, **kwargs) -> PostgresConnection:
    this_connection_override = connection_override.get(None)
    real_kwargs = {}
    if this_connection_override is not None:
        real_kwargs["client"] = this_connection_override

    valid_params = ["autocommit", "dbname"]
    for param in valid_params:
        if param in kwargs:
            real_kwargs[param] = kwargs[param]
    conn = PostgresConnection(**real_kwargs)
    return conn
