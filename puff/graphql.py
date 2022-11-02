import collections.abc
import inspect
from dataclasses import dataclass, fields, is_dataclass
from functools import wraps
from typing import (
    TypeVar,
    Generic,
    Any,
    Optional,
    Dict,
    Callable,
    Union,
    get_origin,
    get_args,
    List,
    get_type_hints,
    Tuple,
    ForwardRef,
    Iterable,
    Iterator,
)

from . import wrap_async, rust_objects
from .postgres import set_connection_override

try:
    from django.db import connection as django_connection
except ImportError:
    django_connection = None


def nested_dataclass(*args, **kwargs):
    def wrapper(cls):
        cls = dataclass(cls, **kwargs)
        original_init = cls.__init__
        cls.__init__ = wrap_method(original_init, cls.__annotations__)
        return cls

    return wrapper(args[0]) if args else wrapper


def parent(parent_field):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # only use a wrapper if you need extra code to be run here
            return func(*args, **kwargs)

        wrapper.parent_field = parent_field
        return wrapper

    return decorator


def needs_puff(func):
    func.safe_without_context = False
    return func


def pure(func):
    func.safe_without_context = True
    return func


def borrow_db_context(func):
    func.safe_without_context = False
    return func


NoneType = type(None)

T = TypeVar("T")


class SQLResponse(Generic[T]):
    def __init__(self, query: str) -> None:
        self.query: str = query


@dataclass
class ParameterDescription:
    """Class for keeping track of an item in inventory."""

    param_type: Any
    default: Any


@dataclass
class FieldDescription:
    """Class for keeping track of an item in inventory."""

    return_type: Any
    arguments: Dict[str, Any]
    is_async: bool = False
    safe_without_context: bool = False
    producer: Optional[Callable[[Any], Any]] = None
    acceptor: Optional[Callable[[Callable[[Any], None]], None]] = None
    depends_on: Optional[List[str]] = None
    default: Any = None


@dataclass
class ObjectDescription:
    """Class for keeping track of an item in inventory."""

    attribute_fields: Dict[str, FieldDescription]
    object_fields: Dict[str, FieldDescription]
    class_fields: Dict[str, FieldDescription]


@dataclass
class TypeDescription:
    """Class for keeping track of an item in inventory."""

    type_info: str
    optional: bool
    inner_type: Optional["TypeDescription"] = None


def type_to_scalar(
    t, all_types, input_types, is_input, optional=False
) -> TypeDescription:
    origin = get_origin(t)

    if origin == Optional:
        optional = True
        t = get_args(t)[0]
        return type_to_scalar(t, all_types, input_types, is_input, optional)
    elif origin == Union and get_args(t)[1] is NoneType:
        optional = True
        t = get_args(t)[0]
        return type_to_scalar(t, all_types, input_types, is_input, optional)

    if origin == list or origin == List:
        return TypeDescription(
            optional=optional,
            type_info="List",
            inner_type=type_to_scalar(get_args(t)[0], all_types, input_types, is_input),
        )
    if t == str:
        return TypeDescription(optional=optional, type_info="String")
    elif t == int:
        return TypeDescription(optional=optional, type_info="Int")
    elif t == float:
        return TypeDescription(optional=optional, type_info="Float")
    elif t == bool:
        return TypeDescription(optional=optional, type_info="Boolean")
    elif t == Any:
        return TypeDescription(optional=optional, type_info="Any")
    elif isinstance(t, str):
        return TypeDescription(optional=optional, type_info=t)
    elif isinstance(t, ForwardRef):
        type_for_forward_ref = str(t)[12:-2]
        return TypeDescription(optional=optional, type_info=type_for_forward_ref)
    elif is_dataclass(t):
        load_aggro_type(t, all_types, input_types, is_input)
        type_name = get_type_name(t)
        return TypeDescription(optional=optional, type_info=type_name)

    raise Exception("Invalid type: {}".format(t))


PARENTS_VAR = "parents"
CONTEXT_VAR = "parents"


def get_type_name(t):
    if isinstance(t, str):
        return t
    type_name = t.__name__
    if hasattr(t, "__typename__"):
        type_name = t.__object_name__
    return type_name


def expand_typehints(type_hints):
    expanded_hints = {}
    for arg_name, arg_field_type in type_hints.items():
        origin = get_origin(arg_field_type)
        if origin == Optional:
            arg_field_type = get_args(arg_field_type)[0]
        elif origin == Union and get_args(arg_field_type)[1] is NoneType:
            arg_field_type = get_args(arg_field_type)[0]
        # Get origin again
        origin = get_origin(arg_field_type)
        is_list = False
        if origin == list or origin == List:
            is_list = True
            inner = get_args(arg_field_type)[0]
            inner_origin = get_origin(inner)
            arg_field_type = inner
            if inner_origin == Optional:
                arg_field_type = get_args(inner_origin)[0]
            elif inner_origin == Union and get_args(inner_origin)[1] is NoneType:
                arg_field_type = get_args(inner_origin)[0]

        expanded_hints[arg_name] = (is_list, arg_field_type)
    return expanded_hints


def wrap_method(method, type_hints):
    expanded_hints = expand_typehints(type_hints)

    @wraps(method)
    def wrapped_method(*args, **kwargs):
        if len(args) == 1:
            ctx = args[0]
            new_args = (GraphQLContext(ctx),)
        else:
            ctx = args[1]
            new_args = (args[0], GraphQLContext(ctx))

        if django_connection is not None:
            django_connection.connection = None
        set_connection_override(ctx.connection())
        for arg_name, arg_value in kwargs.items():
            is_list, arg_field_type = expanded_hints.get(arg_name, None)

            if is_list and is_dataclass(arg_field_type) and isinstance(arg_value, list):
                new_obj = [
                    arg_field_type(**v) if isinstance(v, dict) else v for v in arg_value
                ]
                kwargs[arg_name] = new_obj
            elif is_dataclass(arg_field_type) and isinstance(arg_value, dict):
                new_obj = arg_field_type(**arg_value)
                kwargs[arg_name] = new_obj
        r = method(*new_args, **kwargs)
        set_connection_override(None)
        if django_connection is not None:
            django_connection.connection = None
        return r

    return wrapped_method


def load_aggro_type(t, all_types, input_types, is_input):
    type_name = get_type_name(t)

    properties = {}
    if is_input:
        if type_name in all_types:
            raise Exception(
                f"Tried registering input type {type_name} when it already exists as a normal type"
            )
        if type_name in input_types:
            return
        input_types[type_name] = properties
    else:
        if type_name in input_types:
            raise Exception(
                f"Tried registering type {type_name} when it already exists as an input type"
            )
        if type_name in all_types:
            return
        all_types[type_name] = properties

    for field in fields(t):
        field_t = field.type

        field_type = type_to_scalar(field_t, all_types, input_types, is_input)
        db_column = field.name
        desc = FieldDescription(
            return_type=field_type,
            arguments={},
            depends_on=[db_column],
            safe_without_context=True,
            default=field.default,
        )
        properties[field.name] = desc

    if is_input:
        return

    method_list = inspect.getmembers(t, predicate=inspect.ismethod)
    for (method_name, _method) in method_list:
        if method_name.startswith("_"):
            continue
        method = getattr(t, method_name)
        type_hints = get_type_hints(method)
        signature = inspect.signature(method)
        params = signature.parameters
        if method.__self__ is t:
            arguments = {}
            positional_only = []
            for param_name, param in params.items():
                default = None
                if param.default != inspect.Parameter.empty:
                    default = param.default
                if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                    positional_only.append(param_name)
                    continue
                if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                    annotation = param.annotation
                    if annotation == inspect.Parameter.empty:
                        raise Exception(
                            f"Keyword argument for field {param_name} in {method_name} in type {t.__name__} does not have annotation"
                        )
                    param_t = annotation
                    scalar_t = type_to_scalar(param_t, all_types, input_types, True)
                    arguments[param_name] = ParameterDescription(
                        param_type=scalar_t, default=default
                    )
                else:
                    raise Exception(
                        f"Invalid Parameter {param_name} in {method_name} in type {t.__name__}"
                    )

            if len(positional_only) != 1:
                raise Exception(
                    f"Graphql field expected exactly 1 positional only context arg, instead got {positional_only}"
                )

            if signature.return_annotation == inspect.Signature.empty:
                raise Exception(f"Return typ for Graphql field {method_name} is empty")

            return_t = signature.return_annotation
            origin = get_origin(return_t)
            acceptor = None
            wrapped_method = wrap_method(method, type_hints)
            if origin in (
                Iterator,
                Iterable,
                collections.abc.Iterable,
                collections.abc.Iterator,
            ):
                iterable_args = get_args(return_t)
                return_t = iterable_args[0]
                origin = get_origin(return_t)
                acceptor = make_acceptor(wrapped_method)

            if origin in (Tuple, tuple):
                tuple_args = get_args(return_t)
                tuple_arg_len = len(tuple_args)
                return_t = tuple_args[0]
                if tuple_arg_len == 2:
                    # It is an aligned python list
                    pass
                elif tuple_arg_len in (3, 5):
                    if not (tuple_args[1] == str and get_origin(tuple_args[2]) == list):
                        raise Exception(
                            f"Expected the second argument of a tuple to be a string and the 3rd argument to be a list {method_name}: {tuple_args}"
                        )
                elif tuple_arg_len in (4,):
                    if not (tuple_args[1] == list):
                        raise Exception(
                            f"Expected the second argument of a tuple to be a list {method_name}: {tuple_args}"
                        )
                else:
                    raise Exception(
                        f"Invalid number of tuple arguments for return type of {method_name}: {tuple_args}"
                    )

            depends_on = getattr(method, "depends_on", None)
            safe_without_context = getattr(method, "safe_without_context", False)
            is_async = inspect.iscoroutinefunction(method)
            wrapped_method = wrap_method(method, type_hints)

            desc = FieldDescription(
                return_type=type_to_scalar(return_t, all_types, input_types, is_input),
                safe_without_context=safe_without_context,
                arguments=arguments,
                acceptor=acceptor,
                producer=wrapped_method,
                is_async=is_async,
                depends_on=depends_on,
            )
            properties[method_name] = desc

        else:
            pass


class GraphQLContext:
    def __init__(self, ctx):
        self.ctx = ctx

    @property
    def auth_token(self):
        return self.ctx.auth_token

    @property
    def connection(self):
        return self.ctx.connection()

    def parent_values(self, parent_fields):
        return self.ctx.parent_values(parent_fields)


def make_acceptor(method):
    def acceptor(initiate, ctx, lookahead):
        real_ctx = GraphQLContext(ctx)
        set_connection_override(ctx.connection())
        for result in method(real_ctx, **lookahead):

            def render_fn(*args, **kwargs):
                return result

            wrap_async(lambda r: initiate(r, render_fn), join=True)
        set_connection_override(None)
        if django_connection is not None:
            django_connection.connection = None

    return acceptor


def type_to_description(schema):
    schema_fields = {f.name: f.type for f in fields(schema)}
    all_types = {}
    input_types = {}
    query = schema_fields["query"]
    mutation = schema_fields.get("mutation", None)
    subscription = schema_fields.get("subscription", None)

    load_aggro_type(query, all_types, input_types, False)
    if mutation:
        load_aggro_type(mutation, all_types, input_types, False)
    if subscription:
        load_aggro_type(subscription, all_types, input_types, False)
    return all_types, input_types


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
        """
        Query the configured GraphQL schema.

        Provide an optional connection object to use as the DB connection to query SQL.
        If no connection is specified, a new connection will be used.
        """
        return wrap_async(
            lambda rr: self.client().query(rr, query, variables, conn=connection),
            join=True,
        )


global_graphql = GraphqlClient()
