import datetime
import pandas as pd
from math import floor

import logging
from .Lap import Lap
from .Driver import Driver
from .TyreSet import TyreSet
from .styler import EMPTY_TIME_FIELD
from .utilities import format_time, sum_times, cumsum_times

logger = logging.getLogger('logs/main_log') 

class Stint:
    def __init__(self, stint_number: int,
                 driver: Driver,
                 tyreset: TyreSet,
                 fuel: float | int,
                 track_length: float,
                 break_bias: int = 0):
        """
        Initializes a Stint object.

        Args:
            stint_number (int): The number of the stint.
            driver (Driver): The driver associated with the stint.
            tyreset (TyreSet): The tyre set used for the stint.
            fuel (float | int): The initial fuel amount for the stint.
            track_length (float): The length of the track.
            break_bias (int, optional): The break bias. Defaults to 0.

        Returns:
            None
        """
        lap_columns = ['Input', 'Total Lap', 'Stint Lap', 'Lap Time', 
                'Fuel Used (kg/lap)', 'Fuel Left', 'Energy Lap (MJ)',
                'Energy Total (MJ)', 'Tyre Mileage', 'Notes', 'Time',
                'Comment']
        special_lap_columns = ['Lap Type', 'Fuel Used (kg/lap)',
                              'Fuel Left', 'Tyre Mileage']
        
        self.track_length = track_length
        self.driver = driver
        self.TyreSet = tyreset
        self.lap_columns = lap_columns
        self.special_lap = pd.DataFrame(columns=special_lap_columns)
        self.Laps = pd.DataFrame(columns=lap_columns)
        self.laptimes : list[datetime.timedelta] = []
        self.stint_number = stint_number
        self.start_time  = None
        self.stint_duration = None
        self.fastest_lap = None
        self.average_lap = None
        self.average_fuel_cons = None
        self.average_energy_cons = None
        self.track_weather = None
        self.track_temp = None
        self.air_temp = None
        self.weather = None
        self.track_state = None
        self.break_bias = break_bias
        self.fuel_init = float(fuel)

        self.Laps['Notes'] = self.Laps['Notes'].astype(str)
        self.Laps['Comment'] = self.Laps['Comment'].astype(str)

        self.driver.increment_number_of_stint()
        self.edited_laps = [] # pas trop fan, mais je n'ai pas encore d'autre alternative
        logger.info(f"Stint {self.get_stint_number()} created, driver={self.get_driver().get_acronym()}, tyre={self.get_tyreset().get_set_name()}, fuel={self.get_fuel()}")

    def get_stint_displayed_name(self) -> str:
        """
        Returns the displayed name of the stint.

        The displayed name is formatted as "Stint {stint_number} - {driver_acronym}".

        Returns:
            str: The displayed name of the stint.
        """
        return f"Stint {self.get_stint_number()} - {self.get_driver().get_acronym()}"

    def get_driver(self) -> Driver:
        """
        Returns the driver associated with this Stint object.

        Returns:
            Driver: The driver associated with this Stint object.
        """
        return self.driver
    
    def get_laps(self) -> pd.DataFrame:
        """
        Returns the laps data for the Stint object.

        Returns:
            pd.DataFrame: The laps data for the Stint object.
        """
        return self.Laps
    
    def is_first_stint(self) -> bool:
        """
        Check if the current stint is the first stint.

        Returns:
            bool: True if it is the first stint, False otherwise.
        """
        return self.stint_number == 1

    def get_stint_number(self) -> int:
        """
        Returns the number of the stint.

        Returns:
            int: The number of the stint.
        """
        return self.stint_number
    
    def remove_fuel(self, fuel : float):
        """
        Removes fuel from the initial fuel amount.

        Args:
            fuel (float): The amount of fuel to remove.

        Returns:
            None
        """
        self.fuel_init -= fuel

    def get_tyreset(self) -> TyreSet:
        """
        Returns the tyre set used for the stint.

        Returns:
            TyreSet: The tyre set used for the stint.
        """
        return self.TyreSet
        
    def init_special_lap(self, nb_form_lap: int, cons_grid_lap: float, cons_form_lap : float):
        """
        Initializes the special laps for the stint.

        Args:
            nb_form_lap (int): The number of formation laps.
            cons_grid_lap (float): The fuel consumption per lap to the grid.
            cons_form_lap (float): The fuel consumption per formation lap.

        Returns:
            None
        """
        if not self.get_special_laps().empty: return
        
        # update lap to grid
        self.remove_fuel(cons_grid_lap)
        self.get_tyreset().add_mileage(self.track_length)

        lap_to_grid = {'Lap Type': 'Lap to Grid'}
        lap_to_grid['Fuel Used (kg/lap)'] = cons_grid_lap
        lap_to_grid['Fuel Left'] = self.get_fuel()
        lap_to_grid['Tyre Mileage'] = self.get_tyreset().get_mileage()       
        self.get_special_laps().loc[0] = lap_to_grid
        # update formation lap
        self.get_tyreset().add_mileage(nb_form_lap * self.track_length)
        self.remove_fuel(cons_form_lap)
        
        lap_formation = {'Lap Type': f'Formation Lap (x{nb_form_lap})'}
        lap_formation['Fuel Used (kg/lap)'] = cons_form_lap
        lap_formation['Fuel Left'] = self.get_fuel()
        lap_formation['Tyre Mileage'] = self.get_tyreset().get_mileage()
        self.get_special_laps().loc[1] = lap_formation
        logger.info(f"Special laps for stint {self.get_stint_number()} initialized, nb_formation_lap={nb_form_lap}, cons_grid_lap={cons_grid_lap}, cons_form_lap={cons_form_lap}")

    def get_special_laps(self) -> pd.DataFrame:
        """
        Returns the special laps data for the Stint object.

        Returns:
            pd.DataFrame: The special laps data for the Stint object.
        """
        return self.special_lap

    def get_fuel(self) -> float:
        """
        Returns the remaining fuel amount for the stint.

        Returns:
            float: The remaining fuel amount for the stint.
        """
        return self.fuel_init

    def get_nb_laps(self) -> int:
        """
        Returns the number of laps in the stint.

        Returns:
            int: The number of laps in the stint.
        """
        return len(self.Laps)
    
    def get_driver(self) -> Driver:
        """
        Returns the driver associated with this Stint object.

        Returns:
            Driver: The driver associated with this Stint object.
        """
        return self.driver
    
    def set_start_time(self, start_time: datetime.time) -> None:
        """
        Sets the start time for the stint.

        Args:
            start_time (datetime.time): The start time for the stint.

        Returns:
            None
        """
        self.start_time = datetime.datetime.combine(datetime.date.today(), start_time)
        self.stint_duration = datetime.datetime.combine(datetime.date.today(), start_time)
        self.driver.add_stint_start_time(self.start_time)
        logger.info(f"Start time for stint {self.get_stint_number()} set to {start_time}")

    def get_start_time(self) -> datetime.time:
        """
        Returns the start time of the stint.

        Returns:
            datetime.time: The start time of the stint.
        """
        return self.start_time

    def get_stint_end_time(self) -> datetime.time:
        """
        Returns the end time of the stint.

        Returns:
            datetime.time: The end time of the stint.
        """
        return self.stint_duration
    
    def get_edited_laps(self) -> list[int]:
        """
        Returns the list of lap indices that have been edited.

        Returns:
            list[int]: The list of lap indices that have been edited.
        """
        return self.edited_laps
    
    def add_lap(self, input_:str, 
                laptime: datetime.timedelta,
                fuel_cons: float, total_laps: int, alter = False, lap_index = None) -> None:
        """
        Adds a lap to the stint.

        Args:
            input_ (str): The input for the lap.
            laptime (datetime.timedelta): The lap time.
            fuel_cons (float): The fuel consumption for the lap.
            total_laps (int): The total number of laps.
            alter (bool, optional): Whether the lap is being altered. Defaults to False.
            lap_index (int, optional): The index of the lap being altered. Defaults to None.

        Returns:
            None
        """
        row_ = {
            'Input': input_,
            'Lap Time' : format_time(laptime, show_milliseconds=True),
            'Fuel Used (kg/lap)' : fuel_cons,
            'Notes': '',
            'Comment': ''
        }
        
        index_ = None
        if alter: 
            self.laptimes[lap_index] = laptime
            index_ = lap_index
            row_['Energy Lap (MJ)'] = self.Laps.loc[lap_index, 'Energy Lap (MJ)']
            row_['Energy Total (MJ)'] = self.Laps.loc[lap_index, 'Energy Total (MJ)']
            row_['Notes'] = self.Laps.loc[lap_index, 'Notes']
            row_['Comment'] = self.Laps.loc[lap_index, 'Comment']
            self.edited_laps.append(lap_index)
            logger.info(f"Lap {lap_index} of stint {self.get_stint_number()} edited, laptime={laptime}, fuel_cons={fuel_cons}")
        else :
            self.__add_laptime(laptime)
            index_ = self.get_nb_laps()
            logger.info(f"Lap {index_} of stint {self.get_stint_number()} added, laptime={laptime}, fuel_cons={fuel_cons}")


        self.Laps.loc[index_] = row_
        self.driver.add_driving_time(laptime, update=alter, index=index_)

        self.Laps['Total Lap'] = pd.Series(range(total_laps - self.get_nb_laps() + 1, total_laps + 1))
        self.Laps['Stint Lap'] = pd.Series(range(1, self.get_nb_laps() + 1))
        self.Laps['Tyre Mileage'] = (self.TyreSet.get_mileage() + (self.Laps['Stint Lap']  * self.track_length)).round(2)
        self.Laps['Fuel Left'] = self.get_fuel() - self.Laps['Fuel Used (kg/lap)'].cumsum()
        self.Laps['Time'] = self.__compute_time()

        self.__update_fastest_lap()
        self.__update_average_lap()
        self.__update_average_fuel_consumption()
        self.__update_energy_consumption()
        logger.info(f"Stint {self.get_stint_number()} updated, nb_laps={self.get_nb_laps()}")

    def get_remaining_fuel(self) -> float:
        """
        Returns the remaining fuel amount for the stint.

        Returns:
            float: The remaining fuel amount for the stint.
        """
        if self.get_nb_laps() == 0:
            return self.get_fuel()
        return float(self.Laps['Fuel Left'].iloc[-1])
    
    def remove_lap(self, lap_idx: int, total_laps : int) -> None:
        """
        Removes a lap from the stint.

        Args:
            lap_idx (int): The index of the lap to remove.
            total_laps (int): The total number of laps.

        Returns:
            None
        """
        laptime = self.laptimes.pop(lap_idx)
        self.stint_duration -= laptime
        self.driver.remove_driving_time(lap_idx)
        self.Laps = self.Laps.drop(lap_idx)
        self.Laps.reset_index(drop=True, inplace=True)
        self.Laps['Total Lap'] = pd.Series([i for i in range(total_laps - self.get_nb_laps() + 1, total_laps + 1)])
        self.Laps['Stint Lap'] = pd.Series([i for i in range(1, self.get_nb_laps() + 1)])
        self.Laps['Tyre Mileage'] = (pd.Series([self.TyreSet.get_mileage() for _ in range(self.get_nb_laps())]) + (self.Laps['Stint Lap']  * self.track_length)).round(2)
        self.Laps['Fuel Left'] = pd.Series([self.get_fuel() for _ in range(self.get_nb_laps())]) - self.Laps['Fuel Used (kg/lap)'].cumsum()
        self.Laps['Time'] = self.__compute_time()

        self.__update_fastest_lap()
        self.__update_average_lap()
        self.__update_average_fuel_consumption()
        self.__update_energy_consumption()
        if lap_idx in self.edited_laps:
            self.edited_laps.remove(lap_idx)
        logger.info(f"Stint {self.get_stint_number()} updated, nb_laps={self.get_nb_laps()}")

    def get_stint_duration(self) -> datetime.timedelta:
        """
        Returns the duration of the stint.

        Returns:
            datetime.timedelta: The duration of the stint.
        """
        return (self.stint_duration - self.start_time)

    def get_ith_lap(self, lap_to_get) -> Lap:
        """
        Returns the lap at the specified index.

        Args:
            lap_to_get: The index of the lap to get.

        Returns:
            Lap: The lap at the specified index.
        """
        return self.Laps[lap_to_get]
        
    def get_stint_time_formatted(self) -> str:
        """
        Returns the formatted duration of the stint.

        Returns:
            str: The formatted duration of the stint.
        """
        return format_time(self.get_stint_duration(), show_hour=True)
    
    def add_notes(self, lap_idx: int, note: str) -> None:
        """
        Adds notes to a lap in the stint.

        Args:
            lap_idx (int): The index of the lap to add notes to.
            note (str): The notes to add.

        Returns:
            None
        """
        """
        Adds a note to the specified lap in the stint.

        Parameters:
        lap_idx (int): The index of the lap to add the note to.
        note (str): The note to add.

        Returns:
        None
        """
        self.Laps.loc[lap_idx, 'Notes'] = '' if note is None else note
        self.__update_fastest_lap()
        self.__update_average_lap()
        self.__update_average_fuel_consumption()
        self.__update_energy_consumption()
        logger.info(f"Note added to lap {lap_idx} of stint {self.get_stint_number()}, refresh stats on consumption and lap time")

    def add_comments(self, lap_idx: int, comment: str) -> None:
        """
        Adds a comment to a specific lap in the stint.

        Parameters:
        lap_idx (int): The index of the lap to add the comment to.
        comment (str): The comment to add.

        Returns: None
        """
        self.Laps.loc[lap_idx, 'Comment'] = comment
        logger.info(f"Comment added to lap {lap_idx} of stint {self.get_stint_number()}")
    
    def get_fastest_lap(self) -> datetime.timedelta:
            """
            Returns the fastest lap time for the stint.

            Returns:
                datetime.timedelta: The fastest lap time.
            """
            return self.fastest_lap
    
    def get_fastest_lap_formated(self) -> str:
            """
            Returns the fastest lap time in a formatted string.

            If the fastest lap time is not available, it returns an empty time field.

            Returns:
                str: The fastest lap time in a formatted string.
            """
            input_ = self.get_fastest_lap()
            if input_ is None: return EMPTY_TIME_FIELD
            return format_time(input_, show_milliseconds=True)
    
    def get_average_lap_formated(self) -> str:
            """
            Returns the average lap time formatted as a string.

            If the average lap time is None, returns the string representation of an empty time field.

            Returns:
                str: The average lap time formatted as a string.
            """
            input_ = self.get_average_lap()
            if input_ is None: return EMPTY_TIME_FIELD
            return format_time(input_, show_milliseconds=True)
    
    def get_average_lap(self):
        """
        Returns the average lap time for the stint.

        :return: The average lap time.
        """
        return self.average_lap

    def get_average_fuel_consumption(self) -> float | None:
        """
        Returns the average fuel consumption for the stint.

        Returns:
            float | None: The average fuel consumption for the stint, or None if the value is not available.
        """
        return self.average_fuel_cons

    def get_average_energy_consumption(self) -> float | None:
        """
        Returns the average energy consumption per lap.

        Returns:
            float | None: The average energy consumption per lap if there are values available,
                          otherwise returns None.
        """
        return self.average_energy_cons
    
    def get_fuel_vs_tire(self, fuel_flow: float) -> float | None:
        """
        Calculates the fuel efficiency in terms of fuel used per unit of tire wear.

        Args:
            fuel_flow (float): The fuel flow rate in kg/lap.

        Returns:
            float | None: The fuel efficiency in terms of fuel used per unit of tire wear,
                          rounded to one decimal place. Returns None if the number of laps is zero.

        Raises:
            AssertionError: If the fuel flow rate is zero.
        """
        if self.get_nb_laps() == 0:
            return None
        assert(fuel_flow != 0)
        fuel_used = self.Laps['Fuel Used (kg/lap)'].values.sum()
        fuel_used += self.get_special_laps()['Fuel Used (kg/lap)'].values.sum()
        return round(fuel_used / fuel_flow, 1)
    
    def get_fuel_prediction(self) -> int | None:
            """
            Calculates the predicted number of laps that can be completed based on the average fuel consumption.

            Returns:
                int | None: The predicted number of laps, or None if the average fuel consumption is not available.
            """
            avg_fuel_cons = self.get_average_fuel_consumption()
            if avg_fuel_cons is None: return None
            min_fuel_left = self.Laps['Fuel Left'].min()
            return floor(min_fuel_left / avg_fuel_cons)

    def __update_average_lap(self) -> None:
        """
        Update the average lap time based on the usable lap times.

        This method calculates the average lap time by excluding the lap times
        that have notes associated with them. If there are no usable lap times,
        the average lap time is set to None.

        Returns:
            None
        """
        idx_notes = list(self.Laps[self.Laps['Notes'] != ''].index)
        usable_lap_times = [self.laptimes[i] for i in range(len(self.laptimes)) if i not in idx_notes]
        if usable_lap_times == []:
            self.average_lap = None
            return
        self.average_lap = sum_times(usable_lap_times) / len(usable_lap_times)

    def __update_average_fuel_consumption(self) -> None:
        """
        Update the average fuel consumption based on usable fuel consumption data.

        This method calculates the average fuel consumption by excluding laps with notes
        and considering only the usable fuel consumption data. If there are no laps with
        usable fuel consumption data, the average fuel consumption is set to None.

        Returns:
            None
        """
        idx_notes = list(self.Laps[self.Laps['Notes'] != ''].index)
        usable_fuel_cons = [self.Laps.loc[i, 'Fuel Used (kg/lap)'] for i in range(len(self.Laps)) if i not in idx_notes]
        if usable_fuel_cons == []:
            self.average_fuel_cons = None
            return
        self.average_fuel_cons = sum_times(usable_fuel_cons) / len(usable_fuel_cons)
    
    def __update_energy_consumption(self) -> None:
        """
        Update the energy consumption data for the stint.

        This method calculates the energy consumption per lap and the total energy consumption
        for the stint. The energy consumption is calculated based on the fuel consumption data
        for the stint.

        Returns:
            None
        """
       
        interesting_laps = self.Laps[self.Laps['Energy Lap (MJ)'].notna()]
        idx_notes = list(interesting_laps[interesting_laps['Notes'] != ''].index)
        usable_energy_cons = [interesting_laps.loc[i, 'Energy Lap (MJ)'] for i in interesting_laps.index if i not in idx_notes]
        if usable_energy_cons == []:
            self.average_energy_cons = None
            return
        self.average_energy_cons = sum(usable_energy_cons) / len(usable_energy_cons)

    def __compute_time(self) -> pd.Series:
            """
            Computes the lap times for each lap and returns the cumulative time for each lap.

            Returns:
                pd.Series: A series containing the cumulative time for each lap.
            """
            cumulative_sum = cumsum_times(self.laptimes)
            times = self.start_time + pd.Series(cumulative_sum)
            return times.apply(lambda x: format_time(x.time(), show_hour=True))
    
    def __add_laptime(self, laptime : datetime.timedelta) -> None:
        """
        Adds a laptime to the stint.

        Parameters:
        laptime (datetime.timedelta): The laptime to be added.

        Returns: None
        """
        self.stint_duration += laptime
        self.laptimes.append(laptime)

    def __update_fastest_lap(self) -> None:
        """
        Updates the fastest lap attribute based on the available lap times.

        This method calculates the fastest lap time from the available lap times
        and updates the `fastest_lap` attribute accordingly. If there are no usable
        lap times, the `fastest_lap` attribute is set to None.

        Returns:
            None
        """
        idx_notes = list(self.Laps[self.Laps['Notes'] != ''].index)
        usable_lap_times = [self.laptimes[i] for i in range(len(self.laptimes)) if i not in idx_notes]
        if usable_lap_times == []:
            self.fastest_lap = None
            return
        self.fastest_lap = min(usable_lap_times)

    def request_energy_update(self) -> None:
        """
        Request an update of the energy consumption data for the stint.

        Returns:
            None
        """
        self.__update_energy_consumption()