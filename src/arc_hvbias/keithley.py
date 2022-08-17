"""
Defines a connection to a Kiethley 2400 over serial and provides an interface
to command and query the device
"""
import math
import warnings
from datetime import datetime
from typing import Optional, Tuple

import cothread

from .comms import Comms

MAX_HZ = 20
LOOP_OVERHEAD = 0.03


class Keithley(object):
    def __init__(
        self,
        ip: str = "192.168.0.1",
        port: int = 8080,
    ) -> None:

        self._ip = ip
        self._port = port
        self._comms: Comms = Comms(self._ip, self._port)

        self.sweep_start = datetime.now()
        self.sweep_seconds = 0.0
        self.abort_flag = False

        self.connect()

    def __del__(self) -> None:
        if self._comms is not None:
            self._comms.disconnect()

    def connect(self) -> None:

        if self._comms._socket.getpeername() is None:
            self.disconnect()

        self._comms.connect()

        self._comms.send_receive(b"")
        self._comms.send_receive(b"*RST")

        is_connected, model = self.check_connected()
        if is_connected:
            # set up useful defaults
            self._comms.send_receive(self.startup_commands)
            self.last_recv = ""
        else:
            warnings.warn("Cannot connect to device. Identifier not recognized.")

    def check_connected(self) -> Tuple[bool, Optional[bytes]]:
        # Check the connection
        model = self._comms.send_receive(b"*idn?")
        if model is None or b"MODEL 24" not in model:
            return False, None
        return True, model

    def disconnect(self):
        self._comms.disconnect()

    def get_voltage(self) -> float:
        volts = self._comms.send_receive(b":SOURCE:VOLTAGE?")
        return float(volts) if volts is not None else 0.0

    def set_voltage(self, volts: float) -> None:
        # only allow negative voltages
        volts = math.fabs(volts) * -1
        resp = self._comms.send_receive(f":SOURCE:VOLTAGE {volts}".encode())

    def get_vol_compliance(self) -> float:
        vol_compl = self._comms.send_receive(b":SENSE:VOLTAGE:PROT:LEVEL?")
        return float(vol_compl) if vol_compl is not None else 0.0

    def set_vol_compliance(self, vol_compl: float) -> None:
        vol_compl = math.fabs(vol_compl) * -1
        resp = self._comms.send_receive(
            f":SENSE:VOLTAGE:PROT:LEVEL {vol_compl}".encode()
        )

    def get_current(self) -> float:
        amps = self._comms.send_receive(b":SOURCE:CURRENT?")
        # make it mAmps
        return float(amps) * 1000 if amps is not None else 0.0

    def get_cur_compliance(self) -> float:
        cur_compl = self._comms.send_receive(b":SENSE:CURRENT:PROT:LEVEL?")
        return float(cur_compl) * 1000 if cur_compl is not None else 0.0

    def set_cur_compliance(self, cur_compl: float) -> None:
        cur_compl = math.fabs(cur_compl) / 1000
        resp = self._comms.send_receive(
            f":SENSE:CURRENT:PROT:LEVEL {cur_compl}".encode()
        )

    def source_off(self, _) -> None:
        self._comms.send_receive(b":SOURCE:CLEAR:IMMEDIATE")

    def source_on(self, _) -> None:
        self._comms.send_receive(b":OUTPUT:STATE ON")

    def abort(self) -> None:
        self._comms.send_receive(b":ABORT")
        # come out of sweep mode if we are in it
        self.sweep_seconds = 0
        self.abort_flag = True

    def get_source_status(self) -> int:
        result = self._comms.send_receive(b":OUTPUT:STATE?")
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

        self._comms.send_receive(b":SOURCE:FUNCTION:MODE VOLTAGE")
        self._comms.send_receive(b":SOURCE:VOLTAGE:MODE FIXED")
        for step in range(steps + 1):
            if self.abort_flag:
                break
            self._comms.send_receive(f":SOURCE:VOLTAGE {voltage}".encode())
            self.get_voltage()
            voltage += step_size
            cothread.Sleep(interval)

    startup_commands = b"""
:syst:beep:stat 0
:SENSE:FUNCTION:ON  "CURRENT:DC","VOLTAGE:DC"
:SENSE:CURRENT:RANGE:AUTO 1
:SENSE:VOLTAGE:RANGE:AUTO 1
:SOURCE:VOLTAGE:RANGE:AUTO 1
"""
