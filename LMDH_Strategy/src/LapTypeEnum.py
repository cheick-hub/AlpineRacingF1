from enum import Enum

class LapTypeEnum(Enum):
    SC = "Safety Car"
    FCY = "Full Course Yellow"
    SZ = "Slow Zone"
    IN = "In Lap"
    OUT = "Out Lap"
    FASTLAP = "Fast Lap"
    RF = "Red Flag"