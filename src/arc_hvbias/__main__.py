import asyncio
from argparse import ArgumentParser

from . import __version__
from .ioc import Ioc

__all__ = ["main"]


async def main(args=None):
    parser = ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args(args)

    a = Ioc()
    await a.run_forever()

    # clean up


# test with: pipenv run python -m arc_hvbias
if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(main())
