import os
import subprocess
import sys


def run_cargo():
    my_env = os.environ.copy()
    my_env["PYTHONPATH"] = ":".join(sys.path)
    subprocess.run(["cargo", "run"] + sys.argv[1:], env=my_env)
