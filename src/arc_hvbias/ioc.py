import asyncio
import math
import warnings
from datetime import datetime
from typing import Any, Callable, Coroutine, List, Optional, cast

# Import the basic framework components.
from softioc import builder, softioc

from .decorators import (
    _catch_exceptions,
    _if_connected,
    _if_source_on,
    _loop_forever,
    _shielded,
)
from .keithley import Keithley
from .status import Status

# a global to hold the Ioc instance for interactive access
ioc = None

volt_tol = 5e-1


# for debugging
def tprint(string: str) -> None:
    print(f"{datetime.now()} - {string}")


class AbortException(RuntimeError):
    pass


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
        self.at_voltage_time: Optional[datetime] = None
        self.cycle_stop_time: Optional[datetime] = None
        self.last_transition = datetime.now()
        # self.pause_flag = False
        self.cycle_flag: bool = False
        self.cycle_finished: bool = False

        self.configured: bool = False

        self.abort: bool = False

        self.run_param_status_update = asyncio.Event()
        self.run_update_time_params = asyncio.Event()
        self.run_cycle_control = asyncio.Event()
        # Make sure cycles are not started on boot
        self.run_cycle_control.clear()

        self.task_group_1 = asyncio.TaskGroup()
        self.task_group_2 = asyncio.TaskGroup()
        # self.pause_cycle = cothread.Event()

    async def run_forever(self) -> None:
        """Run the IOC methods continuously."""

        # try:
        #     if getattr(self, "_socket", None) is None:
        #         await self.start_stream()
        # except Exception as e:
        #     print("Exception when starting stream:", e)

        self.running = True

        _task_list: List[asyncio.Task] = []

        tg1: List[Coroutine[Any, Any, Any]] = [
            self.connection_check(),
            self.calculate_healthy(),
            self.set_param_rbvs(),
        ]
        tg2: List[Coroutine[Any, Any, Any]] = [
            self.update_time_params(),
            self.cycle_control(),
            self.check_abort(),
        ]

        async def create_task_group(
            task_group: asyncio.TaskGroup,
            tasks: List[Coroutine[Any, Any, Any]],
            task_group2: asyncio.TaskGroup,
            tasks2: List[Coroutine[Any, Any, Any]],
        ) -> None:
            # handle exceptions
            try:
                async with task_group as tg, task_group2 as tg2:
                    for task in tasks:
                        t = tg.create_task(task)
                        _task_list.append(t)
                    for task in tasks2:
                        t2 = tg2.create_task(task)
                        _task_list.append(t2)
            except* AbortException as err:
                print(f"{err=}")
            await asyncio.sleep(0)

        await create_task_group(self.task_group_1, tg1, self.task_group_2, tg2)

        for i, task in enumerate(_task_list):
            print(f"Task{i}: done={task.done()}, cancelled={task.cancelled()}")

    # -----------------------------------------------------------------------
    #                           Loop Methods
    # -----------------------------------------------------------------------

    @_shielded
    @_loop_forever
    async def connection_check(self) -> None:
        """Loop to constantly check if connected to the device."""

        is_connected = False
        model = ""

        try:
            is_connected, model = await self.k.check_connected()
        except Exception as e:
            print("CheckConnected error")

        if not is_connected:
            self.connected.set(0)
            self.configured = False
            tprint("Connection lost. Attempting to reconnect...")
            await self.k.connect()
        else:
            if not self.connected.get():
                tprint(f"Connected to device: {model}")
                self.connected.set(1)
                await self.configure()

    @_shielded
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

    @_shielded
    @_loop_forever
    @_if_connected
    @_catch_exceptions
    async def set_param_rbvs(self) -> None:
        self.output_rbv.set(await self.k.get_source_status())

        self.vol_compliance_rbv.set(await self.k.get_vol_compliance())
        self.cur_compliance_rbv.set(await self.k.get_cur_compliance())

        # Read data elements and unpack into vars, if Source is ON
        if self.output_rbv.get() == 1:
            volt, curr, res, time, stat = await self.k.read()
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
                self.at_voltage_time = datetime.now()

        if self.at_voltage_time is not None:
            self.time_since = datetime.now() - self.at_voltage_time
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
                    self.run_cycle_control.set()

    @_loop_forever
    @_if_connected
    @_catch_exceptions
    async def cycle_control(self) -> None:
        """
        Perform a depolarisation cycle <repeat> times.
        """
        # Pause thread if flag is False
        if not self.run_cycle_control.is_set():
            # Wait until flag become True
            await self.run_cycle_control.wait()

        self.cycle_failed = False
        self.cycle_finished = False
        # Trigger cycle update loop to begin if the thread is paused
        self.run_update_time_params.set()

        tprint("Start Cycle")
        # If already off, instantly set Time Since to 0 and also Ramp to ON, then wait MAX TIME
        if math.isclose(self.voltage_rbv.get(), 0.0, abs_tol=volt_tol):
            await self.depolarise()

        tprint("Starting depolarisation cycle.")
        self.cycle_flag = True
        self.cycle_rbv.set(True)

        # Begin depolarisation cycle
        for repeat in range(self.repeats.get()):
            await self._cycle(repeat)

        await self._finish_cycle()

        # At end of cycle clear the flag
        self.run_cycle_control.clear()

    @_loop_forever
    @_if_connected
    async def check_abort(self) -> None:
        if self.abort:
            await self.raise_abort()

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
        timer: int = 0
        while timer < self.max_time.get():
            await asyncio.sleep(1)
            timer += 1
            self.time_since_rbv.set(int(timer))

        # Trigger cycle update loop to begin again
        self.cycle_stop_time = datetime.now()
        self.run_update_time_params.set()

    @_if_source_on
    async def do_start_cycle(self, do: int) -> None:
        if do == 1 and not self.cycle_rbv.get():
            # Unpause the cycle control loop
            self.run_cycle_control.set()

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
        await self.k.source_voltage_ramp(voltage, step_size, time)

        self.status_rbv.set(voltage_status)

        # Select the correct wait time based on ramp status
        if ramp_status == Status.RAMP_OFF:
            await asyncio.sleep(self.hold_time.get())
        elif ramp_status == Status.RAMP_ON:
            await asyncio.sleep(self.pause_time.get())

    async def _finish_cycle(self) -> None:
        tprint("Finished cycle.")
        # Return Keithley to IDLE state
        await self.k.abort()

        self.cycle_finished = True
        # If the cycle failed, we do not want to record the end time
        if not self.cycle_failed:
            self.cycle_stop_time = datetime.now()

        self.cycle_rbv.set(False)
        self.cycle_flag = False

    async def raise_abort(self) -> None:
        tprint("raise abort")
        self.k.abort_flag = True
        # Raise error to propagate to other tasks in TaskGroup
        raise AbortException("Abort called.")

    async def do_stop(self, stop: int) -> None:
        # If stop called, abort and Ramp to OFF setpoint
        if stop == 1:
            tprint(f"/!\\ Cycle Aborted.")

            try:
                self.abort = True
            finally:
                # Pause time update thread if Abort exception raised
                self.run_update_time_params.clear()
                # Stop the cycle thread
                self.run_cycle_control.clear()

                async def _tasks_done() -> None:
                    assert isinstance(self.task_group_2._tasks, set)
                    while len(self.task_group_2._tasks) != 0:
                        await asyncio.sleep(1)

                # wait for all tasks to be "cancelled"
                await asyncio.wait_for(_tasks_done(), timeout=None)

                await self.do_ramp_off(1)
                self.cycle_failed = True

                # If stop called before cycle was fully finished, make sure it is
                if not self.cycle_finished:
                    await self._finish_cycle()

                self.at_voltage_time = None
                self.cycle_stop_time = None
                self.time_since_rbv.set(0)

    @_if_source_on
    async def do_ramp_on(self, start: int) -> None:
        tprint("Do RAMP ON")

        self.status_rbv.set(Status.RAMP_ON)
        seconds = self.rise_time.get()
        to_volts = self.on_setpoint.get()
        step_size = self.step_size.get()
        await self.k.source_voltage_ramp(to_volts, step_size, seconds)
        self.status_rbv.set(Status.VOLTAGE_ON)

    @_if_source_on
    async def do_ramp_off(self, start: int) -> None:
        tprint("Do RAMP OFF")

        self.status_rbv.set(Status.RAMP_OFF)
        seconds = self.fall_time.get()
        to_volts = self.off_setpoint.get()
        step_size = self.step_size.get()
        await self.k.source_voltage_ramp(to_volts, step_size, seconds)
        self.status_rbv.set(Status.VOLTAGE_OFF)

    async def configure(self) -> None:
        # Set to bypass arm event detector
        await self.k.arm_direction("SOURCE")
        await self.k.arm_source("IMMEDIATE")

        # Set to bypass trigger event detector
        await self.k.trigger_direction("SOURCE")
        await self.k.trigger_source("IMMEDIATE")

        # Make sure source isn't turned off after a measurement
        await self.k.source_auto_clear("OFF")

        # Set the data elements we want to read back
        await self.k.set_data_elements()

        await self.k.trigger_count(1)

        # Configure string for Keithley function
        conf_func = '"VOLT:DC","CURR:DC"'

        # Send function configure string to Keithley
        await self.k.configure(conf_func)

        # Query if the Keithley is in the correct configuration
        queried = await self.k.query_configure()

        # Wait until Keithley is in the correct configuration
        while queried != conf_func:
            queried = await self.k.query_configure()
            await asyncio.sleep(1)

        self.configured = True
        tprint("Configured!")
