from .LapTypeEnum import LapTypeEnum
from datetime import timedelta

class Lap:
    def __init__(self, fuel_used: float,
                 laptime: timedelta,
                 lap_type: LapTypeEnum = LapTypeEnum.FASTLAP,
                 note: str = '',
                 comment: str = ''):
        self.laptime = laptime
        self.lap_type = lap_type
        self.fuel_used_lap = fuel_used
        self.note = note
        self.comment = comment