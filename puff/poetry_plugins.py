import os
import subprocess
import sys


def run_cargo():
    my_env = os.environ.copy()
    my_env["PYTHONPATH"] = ":".join(sys.path)
    print(":".join(sys.path))
    print(sys.argv[1:])
    subprocess.run(["cargo", "run"] + sys.argv[1:], env=my_env)
