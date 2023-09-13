import math
import warnings
from datetime import datetime
from typing import Union

import cothread

# Import the basic framework components.
from softioc import builder, softioc

from .keithley import Keithley
from .status import Status

# a global to hold the Ioc instance for interactive access
ioc = None


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
        self.cmd_pause = builder.boolOut(
            "PAUSE", always_update=True, on_update=self.do_pause
        )
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
        self.rise_time = builder.aOut("RISE-TIME", initial_value=2, EGU="Sec", PREC=2)
        self.hold_time = builder.aOut("HOLD-TIME", initial_value=3, EGU="Sec", PREC=2)
        self.fall_time = builder.aOut("FALL-TIME", initial_value=2, EGU="Sec", PREC=2)
        self.pause_time = builder.aOut("PAUSE-TIME", initial_value=2, EGU="Sec", PREC=2)
        self.repeats = builder.longOut("REPEATS", initial_value=1)
        self.step_size = builder.aOut("STEP-SIZE", initial_value=5.0)
        self.max_time = builder.longOut("MAX-TIME", initial_value=900)

        # other state variables
        self.last_time: datetime = datetime.fromtimestamp(0)
        self.last_transition = datetime.now()
        self.pause_flag = False
        self.stop_flag = False
        self.cycle_flag = False

        # Boilerplate get the IOC started
        builder.LoadDatabase()
        softioc.iocInit()

        self.pause_param_update = cothread.Event()
        self.pause_cycle_update = cothread.Event()
        self.pause_cycle = cothread.Event()

        cothread.Spawn(self.param_update)
        cothread.Spawn(self.cycle_update)
        cothread.Spawn(self.connection_check)
        # Finally leave the IOC running with an interactive shell.
        softioc.interactive_ioc(globals())

    def connection_check(self) -> None:
        while True:
            is_connected, model = self.k.check_connected()
            if not is_connected:
                self.connected.set(0)
                print("Connection lost. Attempting to reconnect...")
                self.k.connect()
            else:
                if not self.connected.get():
                    print(f"Connected to device: {model}")
                    cothread.Sleep(0.5)
                    self.connected.set(1)
                    self.pause_param_update.Signal()
            cothread.Sleep(0.5)

    # main update loop
    def param_update(self) -> None:
        while True:
            if self.connected.get() == 1:
                try:
                    self.voltage_rbv.set(self.k.get_voltage())
                    self.current_rbv.set(self.k.get_current())
                    self.vol_compliance_rbv.set(self.k.get_vol_compliance())
                    self.cur_compliance_rbv.set(self.k.get_cur_compliance())
                    self.output_rbv.set(self.k.get_source_status())

                    # calculate housekeeping readbacks
                    healthy = (
                        self.output_rbv.get() == 1
                        and self.voltage_rbv.get() == -math.fabs(self.on_setpoint.get())
                    )
                    self.healthy_rbv.set(healthy)

                    # update loop at 2 Hz
                    cothread.Sleep(0.5)
                except ValueError as e:
                    # catch conversion errors when device returns and error string
                    warnings.warn(f"{e}, {self.k.last_recv}")
                    cothread.Yield()
            else:
                # Pause thread while not connected to device
                cothread.Sleep(0.001)
                self.pause_param_update.Wait()

    def cycle_update(self) -> None:
        while True:
            if self.connected.get() == 1 and not self.stop_flag:
                try:
                    if self.cycle_flag == 1:
                        if self.voltage_rbv.get() == -math.fabs(
                            self.off_setpoint.get()
                        ):
                            self.last_time = datetime.now()
                    if self.last_time != datetime.fromtimestamp(0):
                        since = (datetime.now() - self.last_time).total_seconds()
                        self.time_since_rbv.set(int(since))

                        # if max time exceeded since last depolarise then force a cycle
                        if since > self.max_time.get():
                            self.do_start_cycle(do=1)

                    # update loop at 2 Hz
                    cothread.Sleep(0.5)
                except Exception as e:
                    # catch errors
                    warnings.warn(f"{e}, {self.k.last_recv}")
                    cothread.Yield()
            else:
                # Pause thread while not connected to device
                cothread.Sleep(0.001)
                self.pause_cycle_update.AbortWait()

    def do_start_cycle(self, do: int) -> None:
        if do == 1 and not self.cycle_rbv.get():
            self.cycle_flag = True
            cothread.Spawn(self.cycle_control)

    def cycle_control(self) -> None:
        """
        Continuously perform a depolarisation cycle when the detector is idle
        or after max time
        """
        self.stop_flag = False
        self.pause_cycle_update.Signal()

        on_voltage = self.on_setpoint.get()
        off_voltage = self.off_setpoint.get()
        rise_time = self.rise_time.get()
        fall_time = self.fall_time.get()

        repeats = self.repeats.get()

        try:
            self.cycle_rbv.set(True)

            if self.voltage_rbv.get() == 0:
                self.time_since_rbv.set(0)
                self.do_cycle(on_voltage, fall_time, Status.VOLTAGE_ON, Status.RAMP_OFF)

            for repeat in range(repeats):
                self.do_cycle(on_voltage, fall_time, Status.VOLTAGE_ON, Status.RAMP_OFF)

                self.do_cycle(
                    off_voltage, rise_time, Status.VOLTAGE_OFF, Status.RAMP_ON
                )

                self.do_cycle(on_voltage, fall_time, Status.VOLTAGE_ON, Status.RAMP_OFF)

        except RuntimeError as e:
            self.k.voltage_ramp_worker(off_voltage, self.step_size.get(), rise_time)
            self.status_rbv.set(Status.VOLTAGE_OFF)
            self.time_since_rbv.set(0)
            print("cycle failed:", e)

        finally:
            self.cycle_rbv.set(False)
            self.cycle_flag = False

    def do_cycle(
        self,
        voltage: float,
        time: float,
        voltage_status: int,
        ramp_status: int,
    ) -> None:
        step_size = self.step_size.get()

        if self.pause_flag:
            self.pause_cycle.Wait()

        if not self.stop_flag:
            # initially move to a bias-on state
            # self.status_rbv.set(Status.RAMP_ON)
            self.k.voltage_ramp_worker(voltage, step_size, time)

            self.status_rbv.set(voltage_status)

            cothread.Sleep(self.hold_time.get())

            self.status_rbv.set(ramp_status)
        else:
            raise RuntimeError("Abort called.")

    def do_pause(self, pause: int) -> None:
        if pause == 0:
            self.pause_flag = False
            self.cycle_rbv.set(True)
            self.pause_cycle.Signal()
        if pause == 1:
            self.pause_flag = True
            self.cycle_rbv.set(False)

    def do_stop(self, stop: int) -> None:
        if stop == 1:
            self.stop_flag = True
            self.k.abort()
            self.cycle_rbv.set(0)
            self.cmd_off.set(1)
            self.status_rbv.set(Status.VOLTAGE_OFF)
            self.do_ramp_off()

    def do_ramp_on(self, start: bool) -> None:
        self.status_rbv.set(Status.RAMP_ON)
        seconds = self.rise_time.get()
        to_volts = self.on_setpoint.get()
        step_size = self.step_size.get()
        self.k.source_voltage_ramp(to_volts, step_size, seconds)
        self.status_rbv.set(Status.VOLTAGE_ON)

    def do_ramp_off(self, start: bool) -> None:
        self.status_rbv.set(Status.RAMP_OFF)
        seconds = self.fall_time.get()
        to_volts = self.off_setpoint.get()
        step_size = self.step_size.get()
        self.k.source_voltage_ramp(to_volts, step_size, seconds)
        self.status_rbv.set(Status.VOLTAGE_OFF)
