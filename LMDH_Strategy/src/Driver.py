import datetime
from enum import Enum
from .utilities import sum_times

class DriverCategory(Enum):
    Platinum = "Platinum"
    Gold = "Gold"
    Silver = "Silver"
    Bronze = "Bronze"

categoryMapper = {
    "Platinum": DriverCategory.Platinum,
    "Gold": DriverCategory.Gold,
    "Silver": DriverCategory.Silver,
    "Bronze" : DriverCategory.Bronze
}

class Driver:
    def __init__(self, first_name : str, last_name : str, driver_acronym: str, driver_category: DriverCategory):
        self.first_name = first_name
        self.last_name = last_name
        self.acr = driver_acronym
        self.category = driver_category
        self.number_of_stint = 0
        self.stints_start_time : list[datetime.time] = [] # list of start times per stint
        self.driving_time_per_stint : list[list[datetime.timedelta]] = [] # list of times per stint
    
    def init_time_tracker(self):
        """
        Initializes the time tracker for each stint.

        This method appends an empty list to the `driving_time_per_stint` attribute, which will be used to track the driving time for each stint.

        Parameters:
            None

        Returns:
            None
        """
        self.driving_time_per_stint.append([])

    def add_driving_time(self, driving_time_: datetime.timedelta, update: bool, index: int):
        """
        Adds driving time to the current stint.

        Parameters:
        - driving_time_ (datetime.timedelta): The driving time to be added.
        - update (bool): If True, updates the driving time at the specified index. If False, appends the driving time to the current stint.
        - index (int): The index at which to update the driving time (only applicable if update is True).

        Returns:
        None
        """
        current_stint = self.driving_time_per_stint[-1]
        if update:
            current_stint[index] = driving_time_
        else:  
            current_stint.append(driving_time_)

    def get_driving_time(self):
        """
        Returns the driving time per stint for the driver.

        :return: The driving time per stint.
        """
        return self.driving_time_per_stint
    
    def get_total_driving_time(self, per_stint=False) -> datetime.timedelta | list[datetime.timedelta]:
        """
        Calculates the total driving time for the driver.

        Args:
            per_stint (bool, optional): If True, returns the driving time per stint as a list. 
                                        If False, returns the total driving time. 
                                        Defaults to False.

        Returns:
            datetime.timedelta | list[datetime.timedelta]: The total driving time or the driving time per stint.
        """
        sum_of_each_stint = list(map(sum_times, self.driving_time_per_stint))
        return sum_of_each_stint if per_stint else sum_times(sum_of_each_stint)
    
    def get_driving_time_for_stint_i(self, index: int):
        """
        Returns the total driving time for the specified stint index.

        Parameters:
        index (int): The index of the stint.

        Returns:
        float: The total driving time for the specified stint index.
        """
        return sum_times(self.driving_time_per_stint[index])

    def get_acronym(self):
        """
        Returns the acronym associated with the driver.
        
        Returns:
            str: The acronym of the driver.
        """
        return self.acr
    
    def get_first_name(self):
        """
        Returns the first name of the driver.

        Returns:
            str: The first name of the driver.
        """
        return self.first_name
    
    def get_last_name(self):
        """
        Returns the last name of the driver.

        Returns:
            str: The last name of the driver.
        """
        return self.last_name
    
    def get_fullname(self):
        """
        Returns the full name of the driver.

        Returns:
            str: The full name of the driver.
        """
        return f"{self.first_name} {self.last_name}"
    
    def get_category(self):
        """
        Returns the value of the category attribute.

        Returns:
            str: The value of the category attribute.
        """
        return self.category.value
    
    def get_fullname(self):
        """
        Returns the full name of the driver.

        Returns:
            str: The full name of the driver.
        """
        return f"{self.first_name} {self.last_name}"
    
    def get_last_6h_driving_time(self):
        return 0 # TODO
    
    def increment_number_of_stint(self):
        """
        Increments the number of stints for the driver.

        Returns: None
        """
        self.number_of_stint += 1

    def get_number_of_stint(self):
        """
        Returns the number of stints for the driver.
        
        Returns:
            int: The number of stints for the driver.
        """
        return self.number_of_stint

    def add_stint_start_time(self, start_time: datetime.time):
        """
        Adds a start time for a stint to the list of stint start times.

        Parameters:
        start_time (datetime.time): The start time of the stint.

        Returns:
        None
        """
        self.stints_start_time.append(start_time)

    def remove_driving_time(self, index: int):
        """
        Removes the driving time at the specified index in the last stint.

        Parameters:
        - index (int): The index of the element to remove in the last stint.

        Returns:
        - None
        """
        self.driving_time_per_stint[-1].pop(index)
