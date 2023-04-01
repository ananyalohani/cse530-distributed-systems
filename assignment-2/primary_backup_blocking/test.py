import subprocess
import sys
import os
import json
import shutil


def cleanup():
    pattern = "replica_"
    dir = os.getcwd() + "/data"
    for f in os.listdir(dir):
        if f.startswith(pattern):
            shutil.rmtree(os.path.join(dir, f))


if __name__ == "__main__":
    N = sys.argv[1] if len(sys.argv) > 1 else 3
    cwd = os.getcwd()
    commands = ["python registry.py"] + \
        ["python server.py"] * int(N) + ["python client.py"]
    config = {
        'tabs': [
            {
                'cd': cwd,
                'commands': [commands],
            }
        ]
    }
    with open(f'{cwd}/.iterm-workspace', 'w') as f:
        f.write(json.dumps(config))
    cleanup()
    subprocess.check_call(['iterm-workspace'], cwd=cwd)
