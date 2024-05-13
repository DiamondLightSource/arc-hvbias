import subprocess
import sys

from arc_hvbias import __version__


def test_cli_version():
    cmd = [sys.executable, "-m", "arc_hvbias", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__
