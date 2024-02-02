import asyncio
import math
import warnings
from datetime import datetime
from typing import Any, Callable, Coroutine, Optional, Union, cast

# Import the basic framework components.
from softioc import builder, softioc

from .keithley import Keithley
from .status import Status

# a global to hold the Ioc instance for interactive access
ioc = None

volt_tol = 5e-1


# for debugging
def tprint(string: str) -> None:
    print(f"{datetime.now()} - {string}")


_AsyncFuncType = Callable[..., Coroutine[Any, Any, Any]]


class AbortException(RuntimeError):
    pass


def _if_connected(func: _AsyncFuncType) -> _AsyncFuncType:
    """
    Check connection decorator before function call.

    Decorator function to check if the wrapper is connected to the motion controller
    device before calling the attached function.

    Args:
        func (Awaitable): Function to call if connected to device

    Returns:
        Awaitable: The function to wrap func in.

    """

    def check_connection(*args, **kwargs) -> Union[_AsyncFuncType, bool]:
        self = args[0]
        assert isinstance(self, Ioc)
        if not self.connected.get() and not self.configured:
            return True
        return func  # (*args, *kwargs)

    return cast(_AsyncFuncType, check_connection)


def _catch_exceptions(func: _AsyncFuncType) -> _AsyncFuncType:
    async def catch_exceptions(*args, **kwargs) -> None:
        self = args[0]
        assert isinstance(self, Ioc)
        try:
            await func(*args, **kwargs)
            # update loop at 2 Hz
            # await asyncio.sleep(0.5)
        except ValueError as e:
            # catch conversion errors when device returns and error string
            warnings.warn(f"{e}, {self.k.last_recv}")
            # cothread.Yield()
        except AbortException as e:
            pass
        except Exception as e:
            warnings.warn(f"{e}")

    return cast(_AsyncFuncType, catch_exceptions)


async def _loop_forever(func: _AsyncFuncType) -> _AsyncFuncType:
    """Wraps function in a while-true loop.

    Args:
        func (_AsyncFuncType): function to wrap in while-true loop
    """

    async def loop(*args, **kwargs) -> _AsyncFuncType:
        while True:
            await func(*args, *kwargs)
            # update loop at 5 Hz
            await asyncio.sleep(0.2)

    return cast(_AsyncFuncType, loop)


class Ioc:
    """
    A Soft IOC to provide the PVs to control and monitor the Keithley class
    """

    def __init__(
        self,
        ip: str = "192.168.0.1",
        port: int = 8080,
    ):
        # promote the (single) instance for access via commandline
        global ioc
        ioc = self

        # connect to the Keithley via serial
        self.k = Keithley(ip, port)

        # Set the record prefix
        builder.SetDeviceName("BL15J-EA-HV-01")

        self.connected = builder.boolOut("CONNECTED")
        self.connected.set(0)

        # Create some output records (for IOC readouts)
        self.cmd_ramp_off = builder.boolOut(
            "RAMP-OFF", always_update=True, on_update=self.do_ramp_off
        )
        self.cmd_ramp_on = builder.boolOut(
            "RAMP-ON", always_update=True, on_update=self.do_ramp_on
        )
        self.cmd_cycle = builder.boolOut(
            "CYCLE", always_update=True, on_update=self.do_start_cycle
        )
        # self.cmd_pause = builder.boolOut(
        #     "PAUSE", always_update=True, on_update=self.do_pause
        # )
        self.cmd_voltage = builder.aOut(
            "VOLTAGE", always_update=True, on_update=self.k.set_voltage
        )
        self.cmd_off = builder.aOut(
            "OFF", always_update=True, on_update=self.k.source_off
        )
        self.cmd_on = builder.aOut("ON", always_update=True, on_update=self.k.source_on)
        self.cmd_stop = builder.boolOut(
            "STOP", always_update=True, on_update=self.do_stop
        )

        self.voltage_rbv = builder.aIn("VOLTAGE:RBV", EGU="Volts")
        self.current_rbv = builder.aIn("CURRENT:RBV", EGU="mA", PREC=4)
        self.vol_compliance_rbv = builder.aIn("VOLTAGE:COMPLIANCE_RBV", EGU="V")
        self.cur_compliance_rbv = builder.aIn(
            "CURRENT:COMPLIANCE_RBV", EGU="mA", PREC=4
        )
        self.output_rbv = builder.mbbIn("OUTPUT_RBV", "OFF", "ON")
        self.status_rbv = builder.mbbIn("STATUS", *Status.__members__)
        self.healthy_rbv = builder.mbbIn("HEALTHY_RBV", "UNHEALTHY", "HEALTHY")
        self.cycle_rbv = builder.mbbIn("CYCLE_RBV", "IDLE", "RUNNING")
        self.time_since_rbv = builder.longIn("TIME-SINCE", EGU="Sec", initial_value=0)

        # create some input records (for IOC inputs)
        self.on_setpoint = builder.aOut(
            "VOLTAGE:ON-SETPOINT", initial_value=500, EGU="Volts"
        )
        self.off_setpoint = builder.aOut("VOLTAGE:OFF-SETPOINT", EGU="Volts")
        self.vol_compliance = builder.aOut(
            "VOLTAGE:COMPLIANCE",
            EGU="Volts",
            initial_value=21,
            on_update=self.k.set_vol_compliance,
        )
        self.cur_compliance = builder.aOut(
            "CURRENT:COMPLIANCE",
            EGU="mA",
            initial_value=0.105,
            on_update=self.k.set_cur_compliance,
        )
        self.rise_time = builder.aOut("RISE-TIME", initial_value=30, EGU="Sec", PREC=2)
        self.hold_time = builder.aOut("HOLD-TIME", initial_value=3, EGU="Sec", PREC=2)
        self.fall_time = builder.aOut("FALL-TIME", initial_value=30, EGU="Sec", PREC=2)
        self.pause_time = builder.aOut("PAUSE-TIME", initial_value=2, EGU="Sec", PREC=2)
        self.repeats = builder.longOut("REPEATS", initial_value=1)
        self.step_size = builder.aOut("STEP-SIZE", initial_value=5.0)
        self.max_time = builder.longOut("MAX-TIME", initial_value=900)

        # other state variables
        self.cycle_start_time: datetime = datetime.fromtimestamp(0)
        self.cycle_stop_time: Optional[datetime] = None
        self.last_transition = datetime.now()
        # self.pause_flag = False
        self.cycle_flag: bool = False
        self.cycle_finished: bool = False

        self.configured: bool = False

        # Boilerplate get the IOC started
        builder.LoadDatabase()
        softioc.iocInit()

        self.run_param_status_update = asyncio.Event()
        self.run_update_time_params = asyncio.Event()
        # self.pause_cycle = cothread.Event()

    async def run_forever(self) -> None:
        """Run the IOC methods continuously."""

        # try:
        #     if getattr(self, "_socket", None) is None:
        #         await self.start_stream()
        # except Exception as e:
        #     print("Exception when starting stream:", e)

        self.running = True

        await asyncio.gather(
            *[
                self.connection_check,
                self.calculate_healthy,
                self.set_param_rbvs,
                self.update_time_params,
            ]
        )

        # # Finally leave the IOC running with an interactive shell.
        # softioc.interactive_ioc(globals())

    # -----------------------------------------------------------------------
    #                           Loop Methods
    # -----------------------------------------------------------------------

    @_loop_forever
    async def connection_check(self) -> None:
        """Loop to constantly check if connected to the device."""
        is_connected, model = self.k.check_connected()
        if not is_connected:
            self.connected.set(0)
            self.configured = False
            tprint("Connection lost. Attempting to reconnect...")
            self.k.connect()
        else:
            if not self.connected.get():
                tprint(f"Connected to device: {model}")
                await asyncio.sleep(0.5)
                self.connected.set(1)
                await self.configure()

    @_loop_forever
    @_if_connected
    async def calculate_healthy(self) -> None:
        """Determines if the Keithley is in a "Healthy" state.

        This is when all of:
        - Source output is ON
        - Output voltage is at on_setpoint with [volt_tol] tolerance
        - It is NOT currently in a depolarisation cycle
        """
        # calculate housekeeping readbacks
        healthy = (
            self.output_rbv.get() == 1
            and math.isclose(
                self.voltage_rbv.get(),
                -math.fabs(self.on_setpoint.get()),
                abs_tol=volt_tol,
            )
            and not self.cycle_rbv.get()
        )
        self.healthy_rbv.set(healthy)

    @_loop_forever
    @_if_connected
    @_catch_exceptions
    async def set_param_rbvs(self) -> None:
        self.output_rbv.set(self.k.get_source_status())

        self.vol_compliance_rbv.set(self.k.get_vol_compliance())
        self.cur_compliance_rbv.set(self.k.get_cur_compliance())

        # Read data elements and unpack into vars, if Source is ON
        if self.output_rbv.get() == 1:
            volt, curr, res, time, stat = self.k.read()
            self.voltage_rbv.set(float(volt))
            self.current_rbv.set(float(curr))

    @_loop_forever
    @_if_connected
    @_catch_exceptions
    async def update_time_params(self) -> None:
        # Pause thread if flag is False
        if not self.run_update_time_params.is_set():
            # Wait until flag become True
            await self.run_update_time_params.wait()

        # If in a cycle, and voltage is at on_setpoint
        if self.cycle_flag == 1:
            if not math.isclose(
                self.voltage_rbv.get(),
                -math.fabs(self.on_setpoint.get()),
                abs_tol=volt_tol,
            ):
                # Set this is cycle_start_time
                self.cycle_start_time = datetime.now()

        if self.cycle_start_time != datetime.fromtimestamp(0):
            self.time_since = datetime.now() - self.cycle_start_time
            self.time_since_rbv.set(int(self.time_since.total_seconds()))

            # if max time exceeded since last cycle then force a cycle
            if (
                self.cycle_finished
                and not self.cycle_failed
                and self.cycle_stop_time is not None
            ):
                if (
                    datetime.now() - self.cycle_stop_time
                ).total_seconds() > self.max_time.get():
                    await self.do_start_cycle(do=1)

    # -----------------------------------------------------------------------
    #                               Methods
    # -----------------------------------------------------------------------

    @_catch_exceptions
    async def depolarise(self) -> None:
        """Carry out an initial depolarisation cycle with a MAX TIME wait after.

        This is only called if a cycle is started with the voltage at off_setpoint.
        """
        # Pause time param update thread while initial depolarising occurring
        self.run_update_time_params.clear()

        tprint("Set bias ON and wait.")
        await self._do_cycle(
            self.on_setpoint.get(),
            self.fall_time.get(),
            Status.RAMP_ON,
            Status.VOLTAGE_ON,
        )

        tprint(f"Waiting MAX TIME -> {self.max_time.get()}s")
        timer: float = 0.0
        while timer < self.max_time.get():
            await asyncio.sleep(0.5)
            timer += 0.5

        # Trigger cycle update loop to begin again
        self.cycle_stop_time = datetime.now()
        self.run_update_time_params.set()

    async def do_start_cycle(self, do: int) -> None:
        if do == 1 and not self.cycle_rbv.get():
            self.cycle_flag = True
            await self.cycle_control()

            await self._finish_cycle()

    @_catch_exceptions
    async def _cycle(self, repeat: int) -> None:
        tprint(f"Starting Cycle No. {repeat}")
        await self._do_cycle(
            self.off_setpoint.get(),
            self.rise_time.get(),
            Status.RAMP_OFF,
            Status.VOLTAGE_OFF,
        )

        await self._do_cycle(
            self.on_setpoint.get(),
            self.fall_time.get(),
            Status.RAMP_ON,
            Status.VOLTAGE_ON,
        )

    @_catch_exceptions
    async def cycle_control(self) -> None:
        """
        Perform a depolarisation cycle <repeat> times.
        """
        self.cycle_failed = False
        self.cycle_finished = False
        # Trigger cycle update loop to begin if the thread is paused
        self.run_update_time_params.set()

        tprint("Start Cycle")
        # If already off, instantly set Time Since to 0 and also Ramp to ON, then wait MAX TIME
        if math.isclose(self.voltage_rbv.get(), 0.0, abs_tol=volt_tol):
            await self.depolarise()

        tprint("Starting depolarisation cycle.")
        self.cycle_rbv.set(True)

        # Begin depolarisation cycle
        for repeat in range(self.repeats.get()):
            await self._cycle(repeat)

    @_catch_exceptions
    async def _do_cycle(
        self,
        voltage: float,
        time: float,
        ramp_status: int,
        voltage_status: int,
    ) -> None:
        step_size = self.step_size.get()

        self.status_rbv.set(ramp_status)

        # initially move to a bias-on state
        self.k.source_voltage_ramp(voltage, step_size, time)

        self.status_rbv.set(voltage_status)

        # Select the correct wait time based on ramp status
        if ramp_status == Status.RAMP_OFF:
            await asyncio.sleep(self.hold_time.get())
        elif ramp_status == Status.RAMP_ON:
            await asyncio.sleep(self.pause_time.get())

    async def _finish_cycle(self) -> None:
        tprint("Finished cycle.")
        # Return Keithley to IDLE state
        self.k.abort()

        self.cycle_finished = True
        # If the cycle failed, we do not want to record the end time
        if not self.cycle_failed:
            self.cycle_stop_time = datetime.now()

        self.cycle_rbv.set(False)
        self.cycle_flag = False

    async def do_stop(self, stop: int) -> None:
        # If stop called, abort and Ramp to OFF setpoint
        if stop == 1:
            tprint(f"/!\\ Cycle Aborted.")

            try:
                # Raise error to propagate to other loops
                raise AbortException("Abort called.")
            finally:
                # Pause time update thread if Abort exception raised
                self.run_update_time_params.clear()

                self.k.abort_flag = True
                self.do_ramp_off()
                self.cycle_failed = True

                # If stop called before cycle was fully finished, make sure it is
                if not self.cycle_finished:
                    await self._finish_cycle()

                self.cycle_stop_time = None
                self.time_since_rbv.set(0)

    def do_ramp_on(self, start: int) -> None:
        tprint("Do RAMP ON")

        self.status_rbv.set(Status.RAMP_ON)
        seconds = self.rise_time.get()
        to_volts = self.on_setpoint.get()
        step_size = self.step_size.get()
        self.k.source_voltage_ramp(to_volts, step_size, seconds)
        self.status_rbv.set(Status.VOLTAGE_ON)

    def do_ramp_off(self, start: int) -> None:
        tprint("Do RAMP OFF")

        self.status_rbv.set(Status.RAMP_OFF)
        seconds = self.fall_time.get()
        to_volts = self.off_setpoint.get()
        step_size = self.step_size.get()
        self.k.source_voltage_ramp(to_volts, step_size, seconds)
        self.status_rbv.set(Status.VOLTAGE_OFF)

    async def configure(self) -> None:
        # Set to bypass arm event detector
        self.k.arm_direction("SOURCE")
        self.k.arm_source("IMMEDIATE")

        # Set to bypass trigger event detector
        self.k.trigger_direction("SOURCE")
        self.k.trigger_source("IMMEDIATE")

        # Make sure source isn't turned off after a measurement
        self.k.source_auto_clear("OFF")

        # Set the data elements we want to read back
        self.k.set_data_elements()

        self.k.trigger_count(1)

        # Configure string for Keithley function
        conf_func = '"VOLT:DC","CURR:DC"'

        # Send function configure string to Keithley
        self.k.configure(conf_func)

        # Query if the Keithley is in the correct configuration
        queried = self.k.query_configure()

        # Wait until Keithley is in the correct configuration
        while queried != conf_func:
            queried = self.k.query_configure()
            await asyncio.sleep(1)

        self.configured = True
        tprint("Configured!")
