import os
import subprocess
import sys


def cargo():
    my_env = os.environ.copy()
    my_env["PYTHONPATH"] = ":".join(sys.path)
    subprocess.run(["cargo"] + sys.argv[1:], env=my_env)
