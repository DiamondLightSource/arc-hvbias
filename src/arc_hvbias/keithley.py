"""
Defines a connection to a Kiethley 2400 over serial and provides an interface
to command and query the device
"""
import codecs
import math
import warnings
from datetime import datetime
from typing import List, Optional, Tuple, Union

import cothread
from typing_extensions import Literal

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
        self._comms.connect()

        self._comms.send_receive("".encode())
        self._comms.send_receive("*RST".encode())

        is_connected, model = self.check_connected()
        if is_connected:
            # set up useful defaults
            self._comms.send_receive(self.startup_commands)
            self.last_recv = ""
        else:
            warnings.warn("Cannot connect to device. Identifier not recognized.")

    def check_connected(self) -> Tuple[bool, Optional[bytes]]:
        # Check the connection
        model = self._comms.send_receive("*idn?".encode())
        if model is None or "MODEL 24" not in model.decode():
            return False, None
        return True, model

    def disconnect(self):
        self._comms.disconnect()

    def set_data_elements(self) -> None:
        resp = self._comms.send_receive(":ELEM, VOLT, CURR, TIME, STAT".encode())
        query_elements = self._comms.send_receive(":ELEM?".encode())

    def arm_direction(self, direction: str = "SOURCE") -> None:
        resp = self._comms.send_receive(f":ARM:DIR {direction}".encode())

    def trigger_direction(self, direction: str = "SOURCE") -> None:
        resp = self._comms.send_receive(f":TRIGGER:DIR {direction}".encode())

    def arm_source(self, source: str = "IMMEDIATE") -> None:
        resp = self._comms.send_receive(f":ARM:SOURCE {source}".encode())

    def trigger_source(self, source: str = "IMMEDIATE") -> None:
        resp = self._comms.send_receive(f":TRIGGER:SOURCE {source}".encode())

    def arm_count(self, count: Union[int, Literal["INF"]] = 1) -> None:
        assert count >= 1 and count <= 2500 if isinstance(count, int) else "INF"
        resp = self._comms.send_receive(f":ARM:COUNT {count}".encode())

    def trigger_count(self, count: int = 1) -> None:
        assert count >= 1 and count <= 2500
        resp = self._comms.send_receive(f":TRIGGER:COUNT {count}".encode())

    def initiate(self) -> None:
        resp = self._comms.send_receive(":INIT".encode())

    def configure(self, conf_func: str = "VOLT:DC") -> None:
        resp = self._comms.send_receive(f":CONF:{conf_func}".encode())

    def query_configure(self) -> Optional[str]:
        resp = self._comms.send_receive(":CONF?".encode())
        if resp is not None:
            resp = codecs.decode(resp).strip("\n")
        return resp

    def measure(self, measurement: str = "VOLTAGE") -> Optional[bytes]:
        measurement = measurement.upper()
        assert measurement in [
            "VOLT",
            "VOLTAGE",
            "CURR",
            "CURRENT",
            "RES",
            "RESISTANCE",
        ]
        resp = self._comms.send_receive(f":MEASURE:{measurement}?".encode())
        return resp

    def fetch(self) -> List[str]:
        resp = self._comms.send_receive(":FETCH?".encode())
        if resp is not None:
            return codecs.decode(resp).strip("\n").split(",")
        else:
            raise Exception(":FETCH? response is 'None'. Is the source on?")

    def read(self) -> List[str]:
        resp = self._comms.send_receive(":READ?".encode())
        if resp is not None:
            return codecs.decode(resp).strip("\n").split(",")
        else:
            raise Exception(":READ? response is 'None'. Is the source on?")

    def source_auto_clear(self, on_off: str = "ON") -> None:
        on_off = on_off.upper()
        assert on_off in ["ON", "OFF"]
        resp = self._comms.send_receive(f":SOURCE:CLEAR:AUTO {on_off}".encode())

    def get_voltage(self) -> float:
        volts = self._comms.send_receive(":SOURCE:VOLTAGE?".encode())
        return float(volts.decode()) if volts is not None else 0.0

    def set_voltage(self, volts: float) -> None:
        # only allow negative voltages
        volts = math.fabs(volts) * -1
        resp = self._comms.send_receive(f":SOURCE:VOLTAGE {volts}".encode())

    def get_vol_compliance(self) -> float:
        vol_compl = self._comms.send_receive(":SENSE:VOLTAGE:PROT:LEVEL?".encode())
        return float(vol_compl.decode()) if vol_compl is not None else 0.0

    def set_vol_compliance(self, vol_compl: float) -> None:
        vol_compl = math.fabs(vol_compl) * -1
        resp = self._comms.send_receive(
            f":SENSE:VOLTAGE:PROT:LEVEL {vol_compl}".encode()
        )

    def get_current(self) -> float:
        amps = self._comms.send_receive(":SOURCE:CURRENT?".encode())
        # make it mAmps
        return float(amps.decode()) * 1000 if amps is not None else 0.0

    def get_cur_compliance(self) -> float:
        cur_compl = self._comms.send_receive(":SENSE:CURRENT:PROT:LEVEL?".encode())
        return float(cur_compl.decode()) * 1000 if cur_compl is not None else 0.0

    def set_cur_compliance(self, cur_compl: float) -> None:
        cur_compl = math.fabs(cur_compl) / 1000
        resp = self._comms.send_receive(
            f":SENSE:CURRENT:PROT:LEVEL {cur_compl}".encode()
        )

    def source_off(self, _) -> None:
        self._comms.send_receive(":SOURCE:CLEAR:IMMEDIATE".encode())

    def source_on(self, _) -> None:
        self._comms.send_receive(":OUTPUT:STATE ON".encode())

    def abort(self) -> None:
        self._comms.send_receive(":ABORT".encode())
        # come out of sweep mode if we are in it
        self.sweep_seconds = 0
        self.abort_flag = True

    def get_source_status(self) -> int:
        result = self._comms.send_receive(":OUTPUT:STATE?".encode())
        return int(result.decode()) if result is not None else 0

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

        self._comms.send_receive(":SOURCE:FUNCTION:MODE VOLTAGE".encode())
        self._comms.send_receive(":SOURCE:VOLTAGE:MODE FIXED".encode())
        for step in range(steps + 1):
            if self.abort_flag:
                break
            self._comms.send_receive(f":SOURCE:VOLTAGE {voltage}".encode())
            self.get_voltage()
            voltage += step_size
            cothread.Sleep(interval)

    startup_commands = """
:syst:beep:stat 0
:SENSE:FUNCTION:ON  "CURRENT:DC","VOLTAGE:DC"
:SENSE:CURRENT:RANGE:AUTO 1
:SENSE:VOLTAGE:RANGE:AUTO 1
:SOURCE:VOLTAGE:RANGE:AUTO 1
""".encode()
