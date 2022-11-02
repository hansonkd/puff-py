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


def loadb(*args, **kwargs):
    """
    load python object from bytes
    """
    return json_impl.loadb(*args, **kwargs)


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


def dumpb(*args, **kwargs):
    """
    dump python object to bytes
    """
    return json_impl.dumpb(*args, **kwargs)
