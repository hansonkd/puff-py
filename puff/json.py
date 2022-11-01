"""Rust versions of JSON dumping"""

from . import json_impl


def load(*args, **kwargs):
    """
    Hyperjson version of json.load
    """
    return json_impl.load(*args, **kwargs)


def loads(*args, **kwargs):
    """
    Hyperjson version of json.loads
    """
    return json_impl.loads(*args, **kwargs)


def dump(*args, **kwargs):
    """
    Hyperjson version of json.dump
    """
    return json_impl.dump(*args, **kwargs)


def dumps(*args, **kwargs):
    """
    Hyperjson version of json.dumps
    """
    return json_impl.dumps(*args, **kwargs)
