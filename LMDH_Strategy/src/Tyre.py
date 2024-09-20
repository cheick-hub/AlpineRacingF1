from .TyrePositionEnum import TyrePositionEnum
from .Compound import Compound
class Tyre:
    def __init__(self, numero_set : str, Position : TyrePositionEnum, compound : Compound, pression : float, temperature : float, mileage : int = 0):
        self.numero_set = numero_set
        self.Position = Position
        self.mileage = mileage
        self.compound = compound
        self.pression = pression
        self.temperature = temperature

