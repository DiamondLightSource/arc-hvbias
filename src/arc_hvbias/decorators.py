import asyncio
import warnings
from typing import Any, Callable, Coroutine, List, Optional, cast

from .ioc import AbortException, Ioc

_AsyncFuncType = Callable[..., Coroutine[Any, Any, Any]]


def _if_connected(func: _AsyncFuncType) -> _AsyncFuncType:
    """
    Check connection decorator before function call.

    Decorator function to check if the wrapper is connected to the device
    before calling the attached function.

    Args:
        func (_AsyncFuncType): Function to call if connected to device

    Returns:
        _AsyncFuncType: The wrapped function.

    """

    async def check_connection(*args, **kwargs) -> None:
        self = args[0]
        assert isinstance(self, Ioc)
        if self.connected.get() and self.configured:
            await func(*args, **kwargs)

    return cast(_AsyncFuncType, check_connection)


def _if_source_on(func: _AsyncFuncType) -> _AsyncFuncType:
    """
    Check Source is ON before function call.

    Decorator function to check if the device is in the SOURCE ON state
    before calling the attached function.

    Args:
        func (_AsyncFuncType): Function to call if the source is on

    Returns:
        _AsyncFuncType: The wrapped function.

    """

    async def check_source_on(*args, **kwargs) -> None:
        self = args[0]
        assert isinstance(self, Ioc)
        if self.output_rbv.get():
            await func(*args, **kwargs)
        else:
            print("Source is not ON. Please enable the Source before calling this.")

    return cast(_AsyncFuncType, check_source_on)


def _catch_exceptions(func: _AsyncFuncType) -> _AsyncFuncType:
    """Wraps function in a try-catch handler.

    Args:
        func (_AsyncFuncType): function to wrap in try-catch handler

    Returns:
        _AsyncFuncType: The wrapped function
    """

    async def catch_exceptions(*args, **kwargs) -> None:
        self = args[0]
        assert isinstance(self, Ioc)
        try:
            await func(*args, **kwargs)
        except ValueError as e:
            # catch conversion errors when device returns and error string
            warnings.warn(f"{e}, {self.k.last_recv}")
            # cothread.Yield()
        except AbortException as e:
            # pass
            print("AbortException")
        # except asyncio.CancelledError as e:
        #     curr_t = asyncio.current_task()
        #     print(f"Cancel called on {curr_t}, restarting thread...")
        except Exception as e:
            warnings.warn(f"{e}")

    return cast(_AsyncFuncType, catch_exceptions)


def _loop_forever(func: _AsyncFuncType) -> _AsyncFuncType:
    """Wraps function in a while-true loop.

    Args:
        func (_AsyncFuncType): function to wrap in while-true loop

    Returns:
        _AsyncFuncType: The wrapped function
    """

    async def _loop(*args, **kwargs) -> None:
        while True:
            await func(*args, **kwargs)

            # Update at 5Hz
            await asyncio.sleep(0.2)

    return cast(_AsyncFuncType, _loop)


def _shielded(func: _AsyncFuncType) -> _AsyncFuncType:
    """
    Makes so an awaitable method is always shielded from cancellation
    """

    # @functools.wraps(func)
    async def _shield(*args, **kwargs):
        return await asyncio.shield(func(*args, **kwargs))

    return _shield
