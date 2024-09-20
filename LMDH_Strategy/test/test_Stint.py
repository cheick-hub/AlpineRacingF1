import datetime
import unittest
import numpy as np
import pandas as pd
from src.Stint import Stint
from src.TyreSet import TyreSet
from src.Compound import Compound
from src.utilities import InputParser
from src.styler import EMPTY_TIME_FIELD
from src.Driver import Driver, DriverCategory

class TestStint(unittest.TestCase):

    def setUp(self):
        self.driver = Driver(first_name='cheick', last_name='fofana', driver_acronym='CFO', driver_category=DriverCategory.Platinum)
        self.tyreset = TyreSet(set_name='TS1', mileage=10, compound=Compound.HARD)
        self.stint = Stint(stint_number=1, driver=self.driver, tyreset=self.tyreset, fuel=100, track_length=4.3)
        self.start_time = datetime.time(10, 30)
        self.stint.set_start_time(self.start_time)
        self.driver.init_time_tracker()

    def test_get_stint_displayed_name(self):
        self.assertEqual(self.stint.get_stint_displayed_name(), 'Stint 1 - CFO')

    def test_get_driver(self):
        self.assertEqual(self.stint.get_driver(), self.driver)

    def test_get_laps(self):
        self.assertEqual(self.stint.get_nb_laps(), 0)
        self.assertEqual(list(self.stint.get_laps().columns), self.stint.lap_columns)

    def test_is_first_stint(self):
        self.assertTrue(self.stint.is_first_stint())

    def test_get_stint_number(self):
        self.assertEqual(self.stint.get_stint_number(), 1)

    def test_remove_fuel(self):
        self.stint.remove_fuel(10)
        self.assertEqual(self.stint.get_fuel(), 90)

    def test_get_tyreset(self):
        self.assertEqual(self.stint.get_tyreset(), self.tyreset)

    def test_init_special_lap(self):
        nb_formation_lap = 2
        cons_form_laps = 0.5
        cons_to_grid = 1.5
        initial_fuel = self.stint.get_fuel()
        initial_mileage = self.stint.get_tyreset().get_mileage()
    
        self.stint.init_special_lap(nb_formation_lap, cons_to_grid, cons_form_laps)
        special_laps = self.stint.get_special_laps()
        self.assertEqual(len(special_laps), 2)
        self.assertEqual(self.stint.get_fuel(), initial_fuel - (cons_form_laps + cons_to_grid))
        self.assertEqual(self.stint.get_tyreset().get_mileage(), initial_mileage + (nb_formation_lap + 1) * self.stint.track_length)

    def test_get_fuel(self):
        self.assertEqual(self.stint.get_fuel(), 100)

    def test_get_nb_laps(self):
        self.assertEqual(self.stint.get_nb_laps(), 0)

    def test_get_driver(self):
        self.assertEqual(self.stint.get_driver(), self.driver)

    def test_set_start_time(self):
        compared_date = datetime.datetime.combine(datetime.date.today(), self.start_time)
        self.assertEqual(self.stint.get_start_time(), compared_date)

    def test_get_start_time(self):
        self.assertIsNotNone(self.stint.get_start_time())

    def test_get_stint_end_time(self):
        self.assertIsNotNone(self.stint.get_stint_end_time())

    def test_get_edited_laps(self):
        self.assertEqual(self.stint.get_edited_laps(), [])

    def test_add_lap(self):
        input_ = "14578963"
        laptime, fuel_cons = InputParser.parse_lap_input(input_)
        self.stint.add_lap(input_, laptime, fuel_cons, 1)
        laps = self.stint.get_laps()
        self.assertEqual(len(laps), 1)
        self.assertEqual(laps.loc[0, 'Input'], input_)
        self.assertEqual(laps.loc[0, 'Lap Time'], '01:45.78')
        self.assertEqual(laps.loc[0, 'Fuel Used (kg/lap)'], fuel_cons)

    def test_get_remaining_fuel(self):
        self.assertEqual(self.stint.get_remaining_fuel(), 100)

    def test_remove_lap(self):
        input_ = "14578963"
        laptime, fuel_cons = InputParser.parse_lap_input(input_)
        self.stint.add_lap(input_, laptime, fuel_cons, 1)
        self.stint.remove_lap(0, 1)
        laps = self.stint.get_laps()
        self.assertEqual(len(laps), 0)

    def test_get_stint_duration(self):
        self.assertIsNotNone(self.stint.get_stint_duration())

    def test_get_ith_lap(self):
        with self.assertRaises(KeyError): self.stint.get_ith_lap(0) 

    def test_get_fastest_lap(self):
        self.assertIsNone(self.stint.get_fastest_lap())

    def test_get_fastest_lap_formated(self):
        self.assertEqual(self.stint.get_fastest_lap_formated(), EMPTY_TIME_FIELD)

    def test_get_average_lap_formated(self):
        self.assertEqual(self.stint.get_average_lap_formated(), EMPTY_TIME_FIELD)

    def test_get_average_lap(self):
        self.assertIsNone(self.stint.get_average_lap())

    def test_get_average_fuel_consumption(self):
        self.assertIsNone(self.stint.get_average_fuel_consumption())

    def test_get_average_energy_consumption(self):
        self.assertIsNone(self.stint.get_average_energy_consumption())

    def test_get_fuel_vs_tire(self):
        self.assertIsNone(self.stint.get_fuel_vs_tire(2.0))

    def test_get_fuel_prediction(self):
        self.assertIsNone(self.stint.get_fuel_prediction())

    def test_add_notes(self):
        self.stint.add_lap('input', datetime.timedelta(minutes=1, seconds=30), 2.5, 10)
        self.stint.add_notes(0, 'Note')
        laps = self.stint.get_laps()
        self.assertEqual(laps.loc[0, 'Notes'], 'Note')

    def test_add_comments(self):
        self.stint.add_lap('input', datetime.timedelta(minutes=1, seconds=30), 2.5, 10)
        self.stint.add_comments(0, 'Comment')
        laps = self.stint.get_laps()
        self.assertEqual(laps.loc[0, 'Comment'], 'Comment')

    def test_get_average_lap(self):
        self.assertIsNone(self.stint.get_average_lap())

    def test_get_average_fuel_consumption(self):
        self.assertIsNone(self.stint.get_average_fuel_consumption())

    def test_get_average_energy_consumption(self):
        self.assertIsNone(self.stint.get_average_energy_consumption())

    def test_get_fuel_vs_tire(self):
        self.assertIsNone(self.stint.get_fuel_vs_tire(2.0))

    def test_get_fuel_prediction(self):
        self.assertIsNone(self.stint.get_fuel_prediction())

if __name__ == '__main__':
    unittest.main()