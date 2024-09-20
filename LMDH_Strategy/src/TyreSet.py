from .Compound import Compound

import logging
logger = logging.getLogger('logs/main_log')

class TyreSet:
    def __init__(self, set_name: str, mileage: int, compound: Compound):
        self.set_name = set_name
        self.mileage = mileage
        self.initial_mileage = mileage
        self.compound = compound
        logger.info(f"Tyre set {set_name} created with compound {compound.value} and mileage {mileage}")

    def get_set_name(self):
        """
        Returns the name of the tyre set.

        Returns:
            str: The name of the tyre set.
        """
        return self.set_name
    
    def get_mileage(self):
        """
        Returns the current mileage of the tyre set.

        Returns:
            int: The current mileage of the tyre set.
        """
        return self.mileage
    
    def get_initial_mileage(self):
        """
        Returns the initial mileage of the tyre set.

        Returns:
            int: The initial mileage of the tyre set.
        """
        return self.initial_mileage
    
    def add_mileage(self, mileage):
        """
        Adds the given mileage to the current mileage of the tyre set.

        Args:
            mileage (int): The mileage to be added.
        """
        self.mileage += mileage

    def remove_mileage(self, mileage):
        """
        Removes the given mileage from the current mileage of the tyre set.

        Args:
            mileage (int): The mileage to be removed.
        """
        self.mileage -= mileage
    
    def get_compound(self):
        """
        Returns the compound of the tyre set.

        Returns:
            str: The compound of the tyre set.
        """
        return self.compound.value