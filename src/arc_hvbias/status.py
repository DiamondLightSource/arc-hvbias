from enum import IntEnum


class Status(IntEnum):
    VOLTAGE_OFF = 0
    VOLTAGE_ON = 1
    RAMP_OFF = 2
    HOLD = 3
    RAMP_ON = 4
    ERROR = 5
