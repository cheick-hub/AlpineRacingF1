from enum import Enum


class CatanaDataTypeEnum(str, Enum):
    """Enumeration of the data types in Catana2.    
    """
    CDCDATA = 'CDC'
    HISTO2D = 'Histo2D'
    HISTO = 'Histo'
    HISTOLAPDATA = 'HistoLapData'
    LAPDATA = 'LapData'
    OTHER = 'Other'
    RUNDATA = 'RunData'
    CHANNEL = 'Channel'
    METADATA = 'Metadata'
