import subprocess
import sys

from arc_hvbias import __version__

# def test_execution_debug():
#     cmd = [sys.executable, "-m", "arc_hvbias"]
#     result = subprocess.check_output(cmd)
#     cothread.Sleep(1000)
#     print(result.decode())

pvxs_text = "INFO: PVXS QSRV2 is loaded, permitted, and ENABLED."


def test_cli_version():
    cmd = [sys.executable, "-m", "arc_hvbias", "--version"]
    assert subprocess.check_output(cmd).decode().strip(pvxs_text).strip() == __version__
