"""
Defines a connection to a Kiethley 2400 over serial and provides an interface
to command and query the device
"""
import math
import warnings
from datetime import datetime
from typing import Optional, Tuple, Union

import cothread
import serial

MAX_HZ = 20
LOOP_OVERHEAD = 0.03


class Keithley(object):
    def __init__(
        self,
        port: str = "/dev/ttyS0",
        baud: int = 38400,
        bytesize: int = 8,
        parity: str = "N",
    ) -> None:

        self.port = port
        self.baud = baud
        self.bytesize = bytesize
        self.parity = parity
        self.ser: serial.Serial = None

        self.sweep_start = datetime.now()
        self.sweep_seconds = 0.0
        self.abort_flag = False

        self.connect()

    def __del__(self) -> None:
        if hasattr(self, "connection"):
            self.ser.close()

    def connect(self) -> None:

        if self.ser is not None:
            self.disconnect()

        self.ser = serial.Serial(
            self.port,
            self.baud,
            bytesize=self.bytesize,
            parity=self.parity,
            timeout=1,
        )

        # make sure buffers are clear
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()

        # self.send_recv("")
        self.send_recv("*RST")

        is_connected, model = self.check_connected()
        if is_connected:
            # set up useful defaults
            self.send_recv(self.startup_commands)
            self.last_recv = ""
        else:
            # raise ValueError(f"Device Identifier not recognized:")  # {model}")
            warnings.warn(f"Device Identifier not recognized: {model}")

    def check_connected(self) -> Tuple[bool, Optional[str]]:
        # Check the connection
        model = self.send_recv("*idn?")
        if model is None or "MODEL 24" not in model:
            return False, None
        return True, model

    def disconnect(self) -> None:
        if self.ser.isOpen():
            self.ser.close()
        del self.ser

    def send_recv(self, send: str, respond: bool = False) -> Union[str, None]:

        self.ser.write((send + "\n").encode())

        if respond or send.endswith("?"):
            try:
                self.last_recv = self.ser.readline(100).decode()
                response = self.last_recv
                return response
            except UnicodeDecodeError as e:
                warnings.warn(f"{e}")

        return None

    def get_voltage(self) -> float:
        volts = self.send_recv(":SOURCE:VOLTAGE?")
        return float(volts) if volts is not None else 0.0

    def set_voltage(self, volts: float) -> None:
        # only allow negative voltages
        volts = math.fabs(volts) * -1
        resp = self.send_recv(f":SOURCE:VOLTAGE {volts}")

    def get_current(self) -> float:
        amps = self.send_recv(":SOURCE:CURRENT?")
        # make it mAmps
        return float(amps) * 1000 if amps is not None else 0.0

    def source_off(self, _) -> None:
        self.send_recv(":SOURCE:CLEAR:IMMEDIATE")

    def source_on(self, _) -> None:
        self.send_recv(":OUTPUT:STATE ON")

    def abort(self) -> None:
        self.send_recv(":ABORT")
        # come out of sweep mode if we are in it
        self.sweep_seconds = 0
        self.abort_flag = True

    def get_source_status(self) -> int:
        result = self.send_recv(":OUTPUT:STATE?")
        return int(result) if result is not None else 0

    def source_voltage_ramp(
        self, to_volts: float, step_size: float, seconds: float
    ) -> None:
        cothread.Spawn(self.voltage_ramp_worker, *(to_volts, step_size, seconds))

    def voltage_ramp_worker(
        self, to_volts: float, step_size: float, seconds: float
    ) -> None:
        """
        A manual voltage ramp using immediate commands

        This has the benefit of being able to get readbacks during
        the ramp. But the downside is that it cannot be particularly
        fine grained. 20Hz updates is about the limit.

        The alternative is voltage_sweep but that has all sorts of
        issues that are yet to be fixed
        """
        self.abort_flag = False
        voltage = self.get_voltage()
        # only allow negative values
        to_volts = -math.fabs(to_volts)
        difference = to_volts - voltage
        if difference == 0 or seconds <= 0:
            return

        # calculate steps and step_size but limit to 20Hz
        steps = abs(int(difference / step_size))
        if steps / seconds > MAX_HZ:
            steps = int(seconds * MAX_HZ)
        step_size = difference / steps
        interval = seconds / steps - LOOP_OVERHEAD

        self.send_recv(":SOURCE:FUNCTION:MODE VOLTAGE")
        self.send_recv(":SOURCE:VOLTAGE:MODE FIXED")
        for step in range(steps + 1):
            if self.abort_flag:
                break
            self.send_recv(f":SOURCE:VOLTAGE {voltage}")
            self.get_voltage()
            voltage += step_size
            cothread.Sleep(interval)

    startup_commands = """
:syst:beep:stat 0
:SENSE:FUNCTION:ON  "CURRENT:DC","VOLTAGE:DC"
:SENSE:CURRENT:RANGE:AUTO 1
:SENSE:VOLTAGE:RANGE:AUTO 1
:SOURCE:VOLTAGE:RANGE:AUTO 1
"""
