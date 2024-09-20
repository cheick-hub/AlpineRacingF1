import os
import json
import logging
import datetime
import pandas as pd
from .Stint import Stint
from .TyreSet import TyreSet
from .LapTypeEnum import LapTypeEnum
from .Driver import Driver, categoryMapper
from .Compound import compoundMapper
from .utilities import InputParser, format_time, cumsum_times

logger = logging.getLogger('logs/main_log')

class GlobalHandler:
    def __init__(self):
        self.total_laps = 0
        self.Stints : list[Stint] = [] # stints
        self.Drivers : dict[str,Driver] = dict()
        self.TyreSets : dict[str,TyreSet] = dict()
        self.pit_stops : list[datetime.timedelta] = [] # pit_stop time in seconds
        self.stint_counter = 1
        self.current_driver = None
        self.path_tyres = None
        self.path_drivers = None
        logger.info("GlobalHandler created")

    def generate_stint_names(self) -> list[str]:
            """
            Generates a list of displayed names for each stint in the GlobalHandler.

            Returns:
                A list of displayed names for each stint.
            """
            return [st_.get_stint_displayed_name() for st_ in self.Stints]

    def get_pit_stop(self, id_ : int) -> datetime.timedelta:
        """
        Retrieves the pit stop duration for the given ID.

        Parameters:
        id_ (int): The ID of the pit stop.

        Returns:
        datetime.timedelta: The duration of the pit stop.
        """
        return self.pit_stops[id_]
    
    def get_nth_stint(self, id_ : int) -> Stint:
        """
        Retrieve the nth stint from the list of stints.

        Parameters:
        id_ (int): The index of the stint to retrieve.

        Returns:
        Stint: The nth stint from the list of stints.
        """
        return self.Stints[id_]
    
    def __compute_starting_time(self, init_stint_parameters : dict = None) -> datetime.time:
            """
            Computes the starting time for the current stint.

            Args:
                init_stint_parameters (dict, optional): A dictionary containing the initial stint parameters. Defaults to None.

            Returns:
                datetime.time: The starting time for the current stint.
            """
            if not self.has_stint():
                return init_stint_parameters["race_start_time"]
            else:
                last_stint = self.Stints[-1]
                last_stint_end_time = last_stint.get_stint_end_time()
                return (last_stint_end_time + self.pit_stops[-1]).time()
    
    def add_stint(self, driver: Driver,
                  tyre : TyreSet,
                  added_fuel: float,
                  max_capacity : float,
                  track_length: float,
                  has_stint=False,
                  init_stint_parameters : dict = None) -> None:
        """
        Adds a new stint to the GlobalHandler.

        Parameters:
        - driver (Driver): The driver associated with the stint.
        - tyre (TyreSet): The tyre set used for the stint.
        - added_fuel (float): The amount of fuel added to the car for the stint.
        - max_capacity (float): The maximum fuel capacity of the car.
        - track_length (float): The length of the track.
        - has_stint (bool, optional): Indicates if the car has a previous stint. Defaults to False.
        - init_stint_parameters (dict, optional): Initial parameters for the first stint. Defaults to None.

        Returns:
        - None

        """
        stint_id = self.stint_counter
        self.stint_counter += 1
        is_first_stint = (stint_id == 1)
        remaining_fuel = max_capacity

        if has_stint:
            starting_time = self.__compute_starting_time()
            previous_stint = self.get_nth_stint(-1)
            remaining_fuel  = previous_stint.get_remaining_fuel()
            logger.info(f"Remaining fuel from previous stint number={previous_stint.get_stint_number()}: {remaining_fuel}")
        
        total_fuel_in_car = min(remaining_fuel + added_fuel, max_capacity)
        driver.init_time_tracker()
        st = Stint(stint_number=stint_id, driver=driver, tyreset=tyre, fuel=total_fuel_in_car, track_length=track_length)
        
        if is_first_stint:
            nb_form_lap = init_stint_parameters["nb_formation_lap"]
            cons_grid_lap = init_stint_parameters["cons_to_grid_lap"]
            cons_form_lap = init_stint_parameters["cons_formation_lap"]
            st.init_special_lap(nb_form_lap, cons_grid_lap, cons_form_lap)
        
        starting_time = self.__compute_starting_time(init_stint_parameters)
        st.set_start_time(starting_time)
        self.Stints.append(st)
        logger.info(f"Stint number {stint_id} added to the GlobalHandler")

    def add_lap(self, stint_nb: int, input_ : str):
        """
        Adds a lap to the current stint.

        Parameters:
        - stint_nb (int): The number of the current stint.
        - input_ (str): The lap input string.

        Returns:
        None
        """
        laptime, fuel_cons = InputParser.parse_lap_input(input_)
        self.total_laps += 1
        current_stint = self.Stints[stint_nb - 1]
        current_stint.add_lap(input_, laptime, fuel_cons, self.total_laps)

    def remove_lap(self, stint_nb: int, lap_idx: int) -> None:
            """
            Removes a lap from the specified stint.

            Args:
                stint_nb (int): The number of the stint.
                lap_idx (int): The index of the lap to be removed.

            Returns:
                None
            """
            current_stint = self.Stints[stint_nb - 1]
            self.total_laps -= 1
            current_stint.remove_lap(lap_idx, total_laps=self.total_laps)
    
    def add_pit_stops(self, pit_stop_time: str) -> None:
            """
            Adds a pit stop with the specified duration to the list of pit stops.

            Args:
                pit_stop_time (str): The duration of the pit stop.

            Returns:
                None
            """
            pit_stop_time = InputParser.parse_pitstop_time(pit_stop_time)
            self.pit_stops.append(pit_stop_time)
            logger.info(f"Pit stop added with duration {pit_stop_time}")

    def edit_lap(self, stint_nb: int, edited_index: int, edited_input: str):
        """
        Edits a lap in the specified stint.

        Parameters:
            stint_nb (int): The number of the stint to edit (1-indexed).
            edited_index (int): The index of the lap to edit.
            edited_input (str): The edited lap input.

        Returns:
            None
        """
        laptime, fuel_cons = InputParser.parse_lap_input(edited_input)
        current_stint = self.Stints[stint_nb - 1]
        current_stint.add_lap(edited_input, laptime, fuel_cons, self.total_laps, alter=True, lap_index=edited_index)

    def add_driver(self, first_name:str,
                       last_name: str,
                       driver_acr:str,
                       driver_cat:str) -> bool:
            """
            Adds a new driver to the GlobalHandler.

            Args:
                first_name (str): The first name of the driver.
                last_name (str): The last name of the driver.
                driver_acr (str): The acronym of the driver.
                driver_cat (str): The category of the driver.

            Returns:
                bool: True if the driver was successfully added, False otherwise.
            """
            driver_acr = driver_acr.upper()
            driver_cat = categoryMapper[driver_cat]
            driver = Driver(first_name, last_name, driver_acr, driver_cat)
            self.Drivers[driver_acr] = driver
            new_row = {
                "FirstName": first_name,
                "LastName": last_name,
                "Acronyme": driver_acr,
                "Category": driver_cat.value,
                "notes": ""}
            try:
                with open(f'{self.get_driver_path()}/{driver_acr}.json', "w") as f: 
                    json.dump(new_row, f)
                return True
            except Exception as e:
                logger.error(f"Error while adding driver {driver_acr} : {e}")
                return False
    
    def add_drivers(self, drivers: dict[str, Driver]) -> None:
            """
            Adds the given drivers to the GlobalHandler's Drivers dictionary.

            Parameters:
            drivers (dict[str, Driver]): A dictionary containing driver acronyms as keys and Driver objects as values.

            Returns:
            None
            """
            for driver_acr, driver in drivers:
                self.Drivers[driver_acr] = driver

    def add_tyresets(self, sets: list[tuple[str, TyreSet]]) -> None:
            """
            Adds the given tyre sets to the GlobalHandler's TyreSets dictionary.

            Parameters:
            - sets: A list of tuples containing the set name and TyreSet object.

            Returns:
            - None
            """
            for set_name, tyreset in sets:
                self.TyreSets[set_name] = tyreset

    def add_tyreset(self, set_name: str, mileage: int, compound: str) -> bool:
        """
        Adds a new tyre set to the GlobalHandler.

        Args:
            set_name (str): The name of the tyre set.
            mileage (int): The mileage of the tyre set.
            compound (str): The compound of the tyre set.

        Returns:
            bool: True if the tyre set was successfully added, False otherwise.
        """
        set_name = set_name.upper()
        compound = compoundMapper[compound]
        tyreset = TyreSet(set_name=set_name, mileage=mileage, compound=compound)
        self.TyreSets[set_name] = tyreset
        new_row = {"set": set_name, "mileage": mileage, "compound": compound.value, "notes": ""}
        try:
            with open(self.get_tyre_path() + "/" + set_name + ".json", "w") as f:
                json.dump(new_row, f)
            return True
        except Exception as e:
            logger.error(f"Error while adding tyre set {set_name} : {e}")
            return False
    
    def has_stint(self) -> bool:
        """
        Checks if there are any stints available.

        Returns:
            bool: True if there are stints available, False otherwise.
        """
        return len(self.Stints) > 0
    
    def get_nb_stint(self) -> int:
            """
            Returns the number of stints in the GlobalHandler object.

            Returns:
                int: The number of stints.
            """
            return len(self.Stints)
    
    def get_driver(self, driver_acr : str) -> Driver:
        """
        Retrieves the driver object based on the given driver acronym.

        Parameters:
        driver_acr (str): The acronym of the driver.

        Returns:
        Driver: The driver object associated with the given acronym.
        """
        return self.Drivers[driver_acr]
    
    def get_drivers(self) -> dict[str, Driver]:
            """
            Returns a dictionary of drivers.

            Returns:
                dict[str, Driver]: A dictionary containing the drivers.
            """
            return self.Drivers
    
    def get_set(self, set_) -> TyreSet:
        """
        Retrieves the specified TyreSet from the GlobalHandler.

        Args:
            set_ (str): The name of the TyreSet to retrieve.

        Returns:
            TyreSet: The requested TyreSet object.

        """
        return self.TyreSets[set_]

    def get_sets(self) -> dict[str, TyreSet]:
            """
            Returns the dictionary of tyre sets.

            Returns:
                dict[str, TyreSet]: A dictionary containing the tyre sets.
            """
            return self.TyreSets
    
    def add_tyre_path(self, path_tyres):
            """
            Adds the path to the tyre files.

            Args:
                path_tyres (str): The path to the tyre files.

            Returns:
                None
            """
            self.path_tyres = path_tyres

    def add_driver_path(self, path_drivers):
        """
        Adds the specified path to the list of driver paths.

        Args:
            path_drivers (str): The path to be added.

        Returns:
            None
        """
        self.path_drivers = path_drivers
    
    def get_tyre_path(self) -> str:
            """
            Returns the path of the tyre.
            
            Returns:
                str: The path of the tyre.
            """
            return self.path_tyres
    
    def get_driver_path(self) -> str:
            """
            Returns the path to the driver.
            
            Returns:
                str: The path to the driver.
            """
            return self.path_drivers
    
    def load_tyreset(self) -> None:
        """
        Loads tyre sets from JSON files and adds them to the GlobalHandler's list of tyre sets.

        This method reads JSON files from a specified directory, extracts relevant information,
        and creates TyreSet objects. The TyreSet objects are then added to the GlobalHandler's
        list of tyre sets.

        Returns:
            None
        """
        local_path = self.get_tyre_path()
        json_files = list(map(lambda x : local_path + "/" + x, os.listdir(local_path)))
        tyresets = []
        for file in json_files:
            with open(file) as f: 
                tyreset_json = json.load(f)
                tyreset_acc = tyreset_json["set"]
                tyreset = TyreSet(
                    set_name=tyreset_json["set"],
                    mileage=tyreset_json["mileage"],
                    compound=compoundMapper[tyreset_json["compound"]])
                
                tyresets.append((tyreset_acc, tyreset))
        self.add_tyresets(tyresets)

    def load_drivers(self) -> None:
            """
            Loads drivers from JSON files and adds them to the GlobalHandler's drivers list.

            Returns:
                None
            """
            local_path = self.get_driver_path()
            json_files = list(map(lambda x : local_path + "/" + x, os.listdir(local_path)))
            drivers = []
            for file in json_files:
                with open(file) as f: 
                    driver_json = json.load(f)
                    driver_acc = driver_json["Acronyme"]
                    driver = Driver(driver_json["FirstName"],
                                    driver_json["LastName"],
                                    driver_json["Acronyme"],
                                    categoryMapper[driver_json["Category"]])
                    drivers.append((driver_acc, driver))
            self.add_drivers(drivers)

    def generate_dataframe_from_tyres(self, tyres_columns : list) -> pd.DataFrame:
            """
            Generate a dataframe from the tyres dictionary.

            Parameters:
            - tyres_columns (list): A list of column names for the dataframe.

            Returns:
            - pd.DataFrame: The generated dataframe.

            """
            data = []
            tyres = self.get_sets().items()
            for _, tyreset in tyres:  #["set", "mileage", "compound", "notes"]
                data.append((tyreset.get_set_name(), tyreset.get_mileage(), tyreset.get_compound(), ""))
            return pd.DataFrame(data, columns=tyres_columns)
    
    def generate_dataframe_from_drivers(self, drivers_columns : list) -> pd.DataFrame:
            """
            Generate a dataframe from the drivers dictionary.

            Parameters:
            - drivers_columns (list): A list of column names for the dataframe.

            Returns:
            - pd.DataFrame: The generated dataframe.

            """
            data = []
            drivers = self.get_drivers().items()
            for _, driver in drivers:
                data.append((driver.get_first_name(),
                             driver.get_last_name(),
                             driver.get_acronym(),
                             driver.get_category(),
                             ""))

            return pd.DataFrame(data, columns=drivers_columns)

    def add_energy(self, stint_nb:int, lap_idx: int, cons: float) -> None:
        """
        Adds the energy consumption for a specific lap in a stint.

        Parameters:
        stint_nb (int): The number of the stint.
        lap_idx (int): The index of the lap.
        cons (float): The energy consumption for the lap.

        Returns:
        None
        """
        stint_ = self.Stints[stint_nb-1]
        laps = stint_.Laps
        laps.loc[lap_idx, 'Energy Lap (MJ)'] = cons
        stint_.request_energy_update()
        laps['Energy Total (MJ)'] = laps['Energy Lap (MJ)'].astype(float).cumsum()
    
    def add_note(self, stint_nb: int, lap_idx: int, note: str):
        """
        Adds a note to a specific lap in a stint.

        Parameters:
        - stint_nb (int): The number of the stint.
        - lap_idx (int): The index of the lap within the stint.
        - note (str): The note to be added.

        Returns:
        None
        """
        current_stint = self.Stints[stint_nb - 1]
        current_stint.add_notes(lap_idx, note)
    
    def add_comment(self, stint_nb: int, lap_idx: int, comment: str):
        """
        Adds a comment to a specific lap in a stint.

        Parameters:
        - stint_nb (int): The number of the stint.
        - lap_idx (int): The index of the lap within the stint.
        - comment (str): The comment to be added.

        Returns:
        None
        """

        current_stint = self.Stints[stint_nb - 1]
        current_stint.add_comments(lap_idx, comment)

    def get_lap_types(self) -> list[str]:
            """
            Returns a list of lap types.

            Returns:
                list[str]: A list of lap types.
            """
            return LapTypeEnum._member_names_
     
    def generate_stint_recap(self) -> pd.DataFrame:
        """
        Generates a stint recap DataFrame containing information about each stint.

        Returns:
            pd.DataFrame: A DataFrame containing the following columns:
                - Driver: The driver's acronym.
                - Fuel added: The amount of fuel added during the stint.
                - Fuel margin: The fuel margin for the stint.
                - Tire: The tire used during the stint.
                - Stint laps: The number of laps completed in the stint.
                - Race Laps: The cumulative number of laps completed in the race.
                - Fuel InLap: The remaining fuel at the end of the stint.
                - Stint Time: The duration of the stint.
                - Race Time: The cumulative race time up to the end of the stint.
                - <driver_acr>: The cumulative driving time for each driver, where <driver_acr> is the driver's acronym.
        """
        drivers : list[str] = []
        fuel_added  = []
        fuel_margin = []
        stint_laps = []
        stint_time = []
        race_time = []
        in_lap_fuel = []

        current_stint_duration = datetime.timedelta(seconds=0)
        for stint in self.Stints:
            has_lap = stint.get_nb_laps() > 0
            if not has_lap: continue
            current_stint_duration = current_stint_duration + stint.get_stint_duration()
            drivers.append(stint.get_driver().get_acronym())
            stint_laps.append(stint.get_nb_laps())
            race_time.append(format_time(current_stint_duration, show_hour=True, show_milliseconds=False))
            stint_time.append(stint.get_stint_time_formatted())
            in_lap_fuel.append(stint.get_remaining_fuel())

        cumulated_driving_time = self.__compute_cumulated_driving_time(drivers)

        data = dict()
        data["Driver"] = pd.Series(drivers)
        data["Fuel added"] = pd.Series(['-' for _ in range(len(drivers))])
        data["Fuel margin"] = pd.Series([])
        data["Tire"] = pd.Series([])
        data["Stint laps"] = pd.Series(stint_laps)
        data["Race Laps"] = data["Stint laps"].cumsum()
        data["Fuel InLap"] = pd.Series(in_lap_fuel)
        data["Stint Time"] = pd.Series(stint_time)
        data["Race Time"] = pd.Series(race_time)
        
        unique_driver = data["Driver"].unique()
        for driver_acr in unique_driver:
            driver_cumul_driving_time = list(map(lambda x : format_time(x,show_hour=True)\
                                                 , cumsum_times(cumulated_driving_time[driver_acr])))
            data[driver_acr] = pd.Series(driver_cumul_driving_time)

        return pd.DataFrame(data)
    
    def __compute_cumulated_driving_time(self, drivers_order: list[str]) -> dict[str, pd.Series]:
        """
        Computes the cumulated driving time for each driver in the given drivers_order.

        Args:
            drivers_order (list[str]): The order of drivers.

        Returns:
            dict[str, pd.Series]: A dictionary containing the cumulated driving time for each driver.
        """
        cumulated_driving_time: dict[str, list] = dict()
        counter_per_driver = dict()
        drivers = set(drivers_order)

        for driver in drivers:
            cumulated_driving_time[driver] = []
            counter_per_driver[driver] = 0

        for driver_acr in drivers_order:
            for driver_ in cumulated_driving_time.keys():
                if driver_acr == driver_:
                    driver_obj = self.get_driver(driver_acr)
                    driving_time = driver_obj.get_driving_time_for_stint_i(counter_per_driver[driver_acr])
                    cumulated_driving_time[driver_].append(driving_time)
                    counter_per_driver[driver_acr] += 1
                else:
                    cumulated_driving_time[driver_].append(datetime.timedelta(seconds=0))
        return cumulated_driving_time