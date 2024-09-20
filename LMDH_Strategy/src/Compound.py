from enum import Enum

class Compound(Enum):
    SOFT = "Soft"
    MEDIUM = "Medium"
    HARD = "Hard"
    WET = "Wet"


compoundMapper = {
    "Soft": Compound.SOFT,
    "Medium": Compound.MEDIUM,
    "Hard": Compound.HARD,
    "Wet" : Compound.WET
}