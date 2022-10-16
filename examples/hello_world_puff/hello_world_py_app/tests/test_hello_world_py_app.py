from hello_world_py_app import __version__
from puff import global_graphql, global_redis
import puff

gql = global_graphql()


def test_version():
    assert __version__ == "0.1.0"


def test_gql():
    QUERY = """
    query {
        hello_world(my_input: 3) {
            title
            was_input
        }
    }
    """
    result = gql.query(QUERY, {})
    assert "data" in result
    assert "errors" not in result
    assert result["data"]["hello_world"][0]["title"] == "hi from pg"
    assert result["data"]["hello_world"][0]["was_input"] == 3


def test_files():
    fn = "poetry.lock"
    result_bytes = puff.read_file_bytes(fn)  # Puff async function that runs in Tokio.
    result_py_bytes = do_some_blocking_work(
        fn
    )  # Python blocking that spawns a thread to prevent pausing.
    assert len(result_bytes) > 0
    assert len(result_bytes) == len(result_py_bytes)
    assert result_bytes == result_py_bytes


# 100% of python packages are compatible by wrapping them in blocking decorator.
@puff.blocking
def do_some_blocking_work(fn):
    with open(fn, "rb") as f:
        return f.read()
