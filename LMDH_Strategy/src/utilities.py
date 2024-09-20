import datetime
import numpy as np

def format_time(laptime : datetime.timedelta, show_milliseconds = False, show_hour=False) -> str:
    """
    Formats the given laptime into a string representation.

    Parameters:
    laptime (datetime.timedelta): The laptime to be formatted.
    show_milliseconds (bool, optional): Whether to include milliseconds in the formatted time. Defaults to False.
    show_hour (bool, optional): Whether to include hours in the formatted time. Defaults to False.

    Returns:
    str: The formatted laptime as a string.
    """
    time_formatting_split = str(laptime).split('.')
    has_milliseconds = len(time_formatting_split) > 1
    if not show_milliseconds or not has_milliseconds:
        offset = 0 if show_hour else 2
        extra_input = '' if show_hour else '.00'
        return time_formatting_split[0][offset:] + extra_input
    return time_formatting_split[0][2:] + '.' + time_formatting_split[1][:2]

import datetime
import numpy as np

def sum_times(times_: datetime.timedelta) -> datetime.timedelta:
    """
    Sums up a list of time durations.

    Args:
        times_: A list of time durations represented as `datetime.timedelta` objects.

    Returns:
        The sum of all time durations as a `datetime.timedelta` object.
    """
    return np.sum(times_)
def sum_times(times_ : datetime.timedelta) -> datetime.timedelta:
    return np.sum(times_)

def cumsum_times(times_ : list[datetime.timedelta]) -> np.array:
    """
    Calculates the cumulative sum of a list of time durations.

    Parameters:
    times_ (list[datetime.timedelta]): A list of time durations.

    Returns:
    np.array: An array containing the cumulative sum of the time durations.
    """
    return np.cumsum(times_)

class InputParser:
    def __init__(self):
        return
    
    @staticmethod
    def is_valid_hour(hour: str) -> bool:
        """
        Check if the given hour is a valid hour.

        Args:
            hour (str): The hour to be checked.

        Returns:
            bool: True if the hour is valid, False otherwise.
        """
        return hour.isdigit() and int(hour) >= 0 and int(hour) <= 23
    
    @staticmethod
    def if_valid_minute_or_second(minute: str) -> bool:
        """
        Check if the given minute is a valid minute or second.

        Args:
            minute (str): The minute to be checked.

        Returns:
            bool: True if the minute is valid, False otherwise.
        """
        return minute.isdigit() and int(minute) >= 0 and int(minute) <= 59
    
    @staticmethod
    def is_valid_hundredth(hundredth: str) -> bool:
        """
        Check if the given string represents a valid hundredth value.

        Args:
            hundredth (str): The string to be checked.

        Returns:
            bool: True if the string represents a valid hundredth value, False otherwise.
        """
        return (hundredth.isdigit()
                and int(hundredth) >= 0
                and int(hundredth) <= 99)

    @staticmethod
    def parse_lap_input(lap_input: str) -> tuple[datetime.timedelta, float]:
        """
        Parses the lap input string and returns a tuple containing the lap time
        as a `datetime.timedelta` object and the fuel as a float.

        Args:
            lap_input (str): The input string representing the lap time and fuel.
                The input should have 8 digits in the format 'mssddfff', where
                m = minutes, s = seconds, d = decimals, f = fuel.

        Returns:
            tuple[datetime.timedelta, float]: A tuple containing the lap time as
            a `datetime.timedelta` object and the fuel as a float.

        Raises:
            AssertionError: If no input is passed or if the input length is not 8.
            SyntaxError: If the input format is invalid.

        Example:
            >>> parse_lap_input('02345678')
            (datetime.timedelta(minutes=2, seconds=34, milliseconds=560), 0.78)
        """
        assert lap_input != None, "No input passed"
        assert len(lap_input) == 8, """The input should have 8 digits : mssddfff
                                     where m=minutes, s=seconds, d=decimals,
                                     f=fuel"""

        min_ = lap_input[0]
        sec_ = lap_input[1:3]
        decimals_ = lap_input[3:5]
        fuel_ = lap_input[5:]
        fuel = f"{fuel_[0]}.{fuel_[1:]}"

        format_is_ok = (InputParser.if_valid_minute_or_second(min_)
                        and InputParser.if_valid_minute_or_second(sec_)
                        and InputParser.is_valid_hundredth(decimals_)
                        and fuel_.isdigit())
        if not format_is_ok:
            raise SyntaxError(f"""Invalid lap input format : {lap_input} 
                            should be like 'mssddff' with 
                            m=minutes, s=seconds, d=decimals, f=fuel""")

        time_ = datetime.timedelta(
            minutes=int(min_),
            seconds=int(sec_),
            milliseconds=int(decimals_) * 10
        )
        return (time_, float(fuel))

    @staticmethod
    def parse_float_input(float_input: str) -> float:
        """
        Parses a string input and returns it as a float.

        Args:
            float_input (str): The string input to be parsed.

        Returns:
            float: The parsed float value.

        Raises:
            AssertionError: If no input is passed or if the input is not a valid number.
        """
        assert float_input is not None, "No input passed"
        assert float_input.replace(".","").isdigit(), """Energy input is not
                                                         a valid number"""
        return float(float_input)

    @staticmethod
    def parse_pitstop_time(pit_stop_time: str, sep='.') -> datetime.timedelta:
        """
        Parse the pit stop time and return it as a datetime.timedelta object.

        Args:
            pit_stop_time (str): The pit stop time in the format 'mm.ss'.
            sep (str, optional): The separator character between minutes and seconds. Defaults to '.'.

        Returns:
            datetime.timedelta: The parsed pit stop time as a timedelta object.

        Raises:
            ValueError: If the pit stop time is in an invalid format.

        Example:
            >>> parse_pitstop_time('01.30')
            datetime.timedelta(minutes=1, seconds=30)
        """
        assert pit_stop_time is not None, "Pit stop time is None"

        splited_input = pit_stop_time.split(sep)

        if len(splited_input) != 2:
            raise ValueError(f"Invalid pit stop time format: {pit_stop_time}. "
                             f"It should be in the format 'mm{sep}ss'.")

        minutes_ = splited_input[0]
        seconds_ = splited_input[1]
        format_is_ok = (InputParser.if_valid_minute_or_second(minutes_)
                        and InputParser.if_valid_minute_or_second(seconds_))

        if not format_is_ok:
            raise ValueError(f"Invalid pit stop time format: {pit_stop_time}. "
                             f"It should be in the format 'mm{sep}ss'.")

        if len(minutes_) == 1:
            minutes_ = "0" + minutes_
        if len(seconds_) == 1:
            seconds_ = "0" + seconds_

        return datetime.timedelta(minutes=int(minutes_), seconds=int(seconds_))
