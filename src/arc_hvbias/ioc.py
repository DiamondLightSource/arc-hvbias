import math
import warnings
from datetime import datetime

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

    def __init__(self):
        # promote the (single) instance for access via commandline
        global ioc
        ioc = self

        # connect to the Keithley via serial
        self.k = Keithley()

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
        self.cmd_stop = builder.boolOut(
            "STOP", always_update=True, on_update=self.do_stop
        )
        self.cmd_voltage = builder.aOut(
            "VOLTAGE", always_update=True, on_update=self.k.set_voltage
        )
        self.cmd_off = builder.aOut(
            "OFF", always_update=True, on_update=self.k.source_off
        )
        self.cmd_on = builder.aOut("ON", always_update=True, on_update=self.k.source_on)
        self.cmd_abort = builder.boolOut(
            "ABORT", always_update=True, on_update=self.do_abort
        )

        self.voltage_rbv = builder.aIn("VOLTAGE_RBV", EGU="Volts")
        self.current_rbv = builder.aIn("CURRENT_RBV", EGU="mA", PREC=4)
        self.output_rbv = builder.mbbIn("OUTPUT_RBV", "OFF", "ON")
        self.status_rbv = builder.mbbIn("STATUS", *Status.__members__)
        self.healthy_rbv = builder.mbbIn("HEALTHY_RBV", "UNHEALTHY", "HEALTHY")
        self.cycle_rbv = builder.mbbIn("CYCLE_RBV", "IDLE", "RUNNING")
        self.time_since_rbv = builder.longIn("TIME-SINCE", EGU="Sec")

        # create some input records (for IOC inputs)
        self.on_setpoint = builder.aOut("ON-SETPOINT", initial_value=500, EGU="Volts")
        self.off_setpoint = builder.aOut("OFF-SETPOINT", EGU="Volts")
        self.rise_time = builder.aOut("RISE-TIME", initial_value=2, EGU="Sec", PREC=2)
        self.hold_time = builder.aOut("HOLD-TIME", initial_value=3, EGU="Sec", PREC=2)
        self.fall_time = builder.aOut("FALL-TIME", initial_value=2, EGU="Sec", PREC=2)
        self.pause_time = builder.aOut("PAUSE-TIME", EGU="Sec", PREC=2)
        self.repeats = builder.longOut("REPEATS", initial_value=1)
        self.step_size = builder.aOut("STEP-SIZE", initial_value=5.0)
        self.max_time = builder.longOut("MAX-TIME", initial_value=900)

        # other state variables
        self.last_time = datetime.now()
        self.last_transition = datetime.now()
        self.abort_flag = False

        # Boilerplate get the IOC started
        builder.LoadDatabase()
        softioc.iocInit()

        self.pause_update = cothread.Event()
        cothread.Spawn(self.update)
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
                    self.pause_update.Signal()
            cothread.Sleep(0.5)

    # main update loop
    def update(self) -> None:
        while True:
            if self.connected.get() == 1:
                try:
                    self.voltage_rbv.set(self.k.get_voltage())
                    self.current_rbv.set(self.k.get_current())
                    self.output_rbv.set(self.k.get_source_status())

                    # calculate housekeeping readbacks
                    healthy = (
                        self.output_rbv.get() == 1
                        and self.voltage_rbv.get() == -math.fabs(self.on_setpoint.get())
                    )
                    self.healthy_rbv.set(healthy)

                    if self.voltage_rbv.get() == -math.fabs(self.off_setpoint.get()):
                        self.last_time = datetime.now()
                    since = (datetime.now() - self.last_time).total_seconds()
                    self.time_since_rbv.set(int(since))

                    # if max time exceeded since last depolarise then force a cycle
                    if since > self.max_time.get():
                        self.do_start_cycle(do=1)

                    # update loop at 2 Hz
                    cothread.Sleep(0.5)
                except ValueError as e:
                    # catch conversion errors when device returns and error string
                    warnings.warn(f"{e}, {self.k.last_recv}")
                    cothread.Yield()
            else:
                # Pause thread while not connected to device
                cothread.Sleep(0.001)
                self.pause_update.Wait()

    def do_start_cycle(self, do: int) -> None:
        if do == 1 and not self.cycle_rbv.get():
            cothread.Spawn(self.cycle_control)

    def cycle_control(self) -> None:
        """
        Continuously perform a depolarisation cycle when the detector is idle
        or after max time
        """
        self.abort_flag = False

        on_voltage = self.on_setpoint.get()
        off_voltage = self.off_setpoint.get()
        # step_size = self.step_size.get()
        rise_time = self.rise_time.get()
        fall_time = self.fall_time.get()

        repeats = self.repeats.get()

        # on_cycle = on_voltage, fall_time, Status.VOLTAGE_ON, Status.RAMP_UP
        # off_cycle = off_voltage, rise_time, Status.VOLTAGE_OFF, Status.RAMP_DOWN

        try:
            self.cycle_rbv.set(True)

            for repeat in range(self.repeats.get()):

                self.do_cycle(on_voltage, fall_time, Status.VOLTAGE_ON, Status.RAMP_UP)

                # if repeat > 0:
                #     cothread.Sleep(self.hold_time.get())

                self.healthy_rbv.set(False)
                self.do_cycle(
                    off_voltage, rise_time, Status.VOLTAGE_OFF, Status.RAMP_DOWN
                )

                self.do_cycle(on_voltage, fall_time, Status.VOLTAGE_ON, Status.RAMP_UP)

        except Exception as e:
            print("cycle failed", e, self.k.last_recv)

        finally:
            self.cycle_rbv.set(False)

    def do_cycle(
        self,
        voltage: float,
        time: float,
        voltage_status: int,
        ramp_status: int,
    ) -> None:

        # voltage, time, voltage_status, ramp_status = args

        step_size = self.step_size.get()

        if not self.abort_flag:

            # initially move to a bias-on state
            # self.status_rbv.set(Status.RAMP_DOWN)
            self.k.voltage_ramp_worker(voltage, step_size, time)

            self.status_rbv.set(voltage_status)

            cothread.Sleep(self.hold_time.get())

            self.status_rbv.set(ramp_status)
            self.healthy_rbv.set(False)

    def do_stop(self, stop: int) -> None:
        if stop == 0:
            self.stop_flag = False
            self.cycle_rbv.set(True)
        if stop == 1:
            self.stop_flag = True
            self.cycle_rbv.set(False)

    def do_abort(self, abort: int) -> None:
        if abort == 1:
            self.abort_flag = True
            self.cmd_abort.set(1)
            self.cycle_rbv.set(0)
            self.cmd_off.set(1)
            self.status_rbv.set(Status.VOLTAGE_OFF)

    def do_ramp_on(self, start: bool) -> None:
        self.status_rbv.set(Status.RAMP_DOWN)
        seconds = self.rise_time.get()
        to_volts = self.on_setpoint.get()
        step_size = self.step_size.get()
        self.k.source_voltage_ramp(to_volts, step_size, seconds)

    def do_ramp_off(self, start: bool) -> None:
        self.status_rbv.set(Status.RAMP_UP)
        seconds = self.fall_time.get()
        to_volts = self.off_setpoint.get()
        step_size = self.step_size.get()
        self.k.source_voltage_ramp(to_volts, step_size, seconds)
