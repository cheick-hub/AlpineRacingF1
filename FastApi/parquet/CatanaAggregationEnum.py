# min, max, first, last, mean, std, any, all, count, median, prod, sum, var, None

from enum import Enum


class CatanaAggregationEnum(str, Enum):
    """
        Enum of all aggregation authorized.
    """
    ALL = 'all'
    ANY = 'any'
    COUNT = 'count'
    FIRST = 'first'
    LAST = 'last'
    MAX = 'max'
    MEAN = 'mean'
    MEDIAN = 'median'
    MIN = 'min'
    NONE = 'none'
    PRODUCT = 'prod'
    STDDEV = 'std'
    SUM = 'sum'
    VARIANCE = 'var'