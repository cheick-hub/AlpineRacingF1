import os
import unittest
import numpy as np
import pandas as pd
from src.Stint import Stint
from src.Compound import Compound
from datetime import time, timedelta
from src.Driver import DriverCategory
from src.GlobalHandler import GlobalHandler

DATA_PATH = "test/data"
TYRE_PATH = DATA_PATH + "/tyres"
DRIVER_PATH = DATA_PATH + "/drivers"

if not os.path.exists(DATA_PATH): os.makedirs(DATA_PATH)
if not os.path.exists(TYRE_PATH): os.makedirs(TYRE_PATH)
if not os.path.exists(DRIVER_PATH): os.makedirs(DRIVER_PATH)

class TestGlobalHandler(unittest.TestCase):

    def setUp(self):
        self.tyre_path = TYRE_PATH
        self.driver_path = DRIVER_PATH
        self.global_handler = GlobalHandler()
        self.global_handler.add_tyre_path(self.tyre_path)
        self.global_handler.add_driver_path(self.driver_path)
        self.create_drivers()
        self.create_sets()
        self.start_time = time(11, 0)

    def tearDown(self):
        for file in os.listdir(self.tyre_path):
            os.remove(os.path.join(self.tyre_path, file))
        for file in os.listdir(self.driver_path):
            os.remove(os.path.join(self.driver_path, file))

    def create_drivers(self):
        add_1 : bool = self.global_handler.add_driver(first_name="Cheick", last_name="Fofana",
                        driver_acr="CFO", driver_cat=DriverCategory.Gold.value)
        add_2 : bool = self.global_handler.add_driver(first_name="Mouctar", last_name="Fofana",
                        driver_acr="MFO", driver_cat=DriverCategory.Gold.value)
        
        assert add_1, "Could not add driver 1"
        assert add_2, "Could not add driver 2"

    def create_sets(self):
        add_1 : bool = self.global_handler.add_tyreset(set_name="TS1",
                                                       mileage=10, compound=Compound.HARD.value) 
        add_2 : bool = self.global_handler.add_tyreset(set_name="TS2",
                                                       mileage=1, compound=Compound.HARD.value) 
        assert add_1, "Could not add set 1"
        assert add_2, "Could not add set 2"

    def create_stint_initial_stint(self):
        fuel = 70
        has_stint = False
        fuel_added = 0
        track_length = 5.44
        cons_to_grid_lap = 2.5
        nb_formation_lap = 1
        cons_form_lap = 1.5
        driver_acr = list(self.global_handler.get_drivers().keys())[0]
        tyre_set_name = list(self.global_handler.get_sets().keys())[0]
        driver = self.global_handler.get_driver(driver_acr)
        tyre = self.global_handler.get_set(tyre_set_name)
        add_stint_parameters = {
            "driver" : driver,
            "tyre" : tyre,
            "added_fuel" : fuel_added,
            "max_capacity" : fuel,
            "has_stint" : has_stint,
            "track_length" : track_length,
            "init_stint_parameters" : {
                "cons_to_grid_lap" : cons_to_grid_lap,
                "nb_formation_lap" : nb_formation_lap,
                "cons_formation_lap" : cons_form_lap,
                "race_start_time" : self.start_time}
        }
        self.global_handler.add_stint(**add_stint_parameters)
        remaining_fuel = fuel - (cons_to_grid_lap + cons_form_lap)
        return remaining_fuel, driver, tyre, self.start_time, track_length

    def add_lap_to_stint(self, stint : Stint, input_ : str):
        self.global_handler.add_lap(stint.get_stint_number(), input_)

    def test_load_tyreset(self):
        lst = os.listdir(self.tyre_path)
        number_files = len(lst)
        self.assertEqual(len(self.global_handler.TyreSets), number_files)
    
    def test_load_drivers(self):
        lst = os.listdir(self.driver_path)
        number_files = len(lst)
        self.assertEqual(len(self.global_handler.Drivers), number_files)

    def test_add_stint_initial(self):
        remaining_fuel, driver, tyre, race_time_start, _ = self.create_stint_initial_stint()
        self.assertEqual(self.global_handler.get_nb_stint(), 1)
        created_stint = self.global_handler.get_nth_stint(-1)
        self.assertEqual(created_stint.get_driver(), driver)
        self.assertEqual(created_stint.get_tyreset(), tyre)
        self.assertEqual(created_stint.get_start_time().time(), race_time_start)
        self.assertEqual(created_stint.get_fuel(), remaining_fuel)

    def test_add_lap(self):
        input_ = "14578963"
        remaining_fuel, _, _, _, track_legnth = self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(created_stint, input_)
        
        self.assertEqual(created_stint.get_nb_laps(), 1)
        laps = created_stint.get_laps()
        self.assertEqual(laps['Input'][0], input_)
        self.assertEqual(laps['Total Lap'][0], 1)
        self.assertEqual(laps['Stint Lap'][0], 1)
        self.assertEqual(laps['Fuel Used (kg/lap)'][0], 9.63)
        self.assertEqual(laps['Fuel Left'][0], remaining_fuel - laps['Fuel Used (kg/lap)'][0])
        self.assertTrue(np.all(laps['Time'][0]==pd.Series(["11:01:45"])))
        self.assertTrue(np.isclose(laps['Tyre Mileage'][0], created_stint.get_tyreset().get_mileage() + track_legnth))
    
    def test_edit_lap(self):
        input_ = "14578963"
        self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(created_stint, input_)

        input_2 = "15578964"
        self.global_handler.edit_lap(stint_nb=created_stint.get_stint_number(),\
                                      edited_index=0, edited_input=input_2)
        
        self.assertEqual(created_stint.get_nb_laps(), 1)
        laps = created_stint.get_laps()
        self.assertEqual(laps['Input'][0], input_2)
        self.assertEqual(laps['Total Lap'][0], 1)
        self.assertEqual(laps['Stint Lap'][0], 1)
        self.assertEqual(laps['Fuel Used (kg/lap)'][0], 9.64)
        self.assertTrue(np.all(laps['Time'][0]==pd.Series(["11:01:55"])))

    def test_add_energy(self):
        input_ = "14578963"
        input_2= "15578964"
        self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(created_stint, input_)
        self.add_lap_to_stint(created_stint, input_2)

        cons = 1.5
        self.global_handler.add_energy(stint_nb=1, lap_idx=0, cons=cons)
        self.global_handler.add_energy(stint_nb=1, lap_idx=1, cons=cons)
        laps = created_stint.get_laps()
        self.assertEqual(laps['Energy Lap (MJ)'][0], cons)
        self.assertEqual(laps['Energy Lap (MJ)'][1], cons)
        self.assertEqual(laps['Energy Total (MJ)'][0], cons)
        self.assertEqual(laps['Energy Total (MJ)'][1], cons + cons)

    def test_add_note(self):
        input_ = "14578963"
        self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(created_stint, input_)
        note = "This is a note"
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note=note)
        laps = created_stint.get_laps()
        self.assertEqual(laps['Notes'][0], note)

    def test_add_comment(self):
        input_ = "14578963"
        self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(created_stint, input_)
        comment = "This is a comment"
        self.global_handler.add_comment(stint_nb=1, lap_idx=0, comment=comment)
        laps = created_stint.get_laps()
        self.assertEqual(laps['Comment'][0], comment)
    
    def test_remove_lap(self):
        input_ = "14578963"
        self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(created_stint, input_)

        self.global_handler.remove_lap(stint_nb=1, lap_idx=0)
        self.assertEqual(created_stint.get_nb_laps(), 0)
        laps = created_stint.get_laps()
        self.assertEqual(laps.shape[0], 0)

    def test_add_pitstop(self):
        pit_stop_time = "1.30"
        self.global_handler.add_pit_stops(pit_stop_time=pit_stop_time)
        self.assertEqual(self.global_handler.pit_stops[0], timedelta(minutes=1, seconds=30))

    def test_get_pit_stop(self):
        pit_stop_time = "1.30"
        self.global_handler.add_pit_stops(pit_stop_time=pit_stop_time)
        self.assertEqual(self.global_handler.get_pit_stop(0), timedelta(minutes=1, seconds=30))

    def test_add_stint(self):
        input_ = "14578963"
        _, _, _, _, track_length = self.create_stint_initial_stint()
        init_stint = self.global_handler.get_nth_stint(-1)
        self.add_lap_to_stint(init_stint, input_)

        pit_stop_time = "1.30"
        self.global_handler.add_pit_stops(pit_stop_time=pit_stop_time)

        max_capacity = 70
        driver_acr = list(self.global_handler.get_drivers().keys())[1]
        tyre_set_name = list(self.global_handler.get_sets().keys())[1]
        driver = self.global_handler.get_driver(driver_acr)
        tyre = self.global_handler.get_set(tyre_set_name)
        add_stint_parameters = {
            "driver" : driver,
            "tyre" : tyre,
            "added_fuel" : max_capacity + 40, # just to test the case where the fuel added is greater than the max capacity
            "max_capacity" : max_capacity,
            "has_stint" : self.global_handler.has_stint(),
            "track_length" : track_length,
            "init_stint_parameters" : None
        }
        self.global_handler.add_stint(**add_stint_parameters)
        self.assertEqual(self.global_handler.get_nb_stint(), 2)
        created_stint = self.global_handler.get_nth_stint(-1)
        self.assertEqual(created_stint.get_driver(), driver)
        self.assertEqual(created_stint.get_tyreset(), tyre)
        self.assertEqual(created_stint.get_fuel(), max_capacity)
        self.assertEqual(created_stint.get_nb_laps(), 0)
        self.add_lap_to_stint(created_stint, input_)
        self.assertTrue(np.all(created_stint.Laps['Time'][0]==pd.Series(["11:05:01"]))) # (start) 11:00:00 + (st1-lap):01:45.78 + (pitsop):01:30 + (st2-lap):01:45.78

    def test_generate_stint_names(self):
        _, driver, _, _, _ = self.create_stint_initial_stint()
        created_stint = self.global_handler.get_nth_stint(-1)
        driver_acr = driver.get_acronym()
        self.assertEqual(self.global_handler.generate_stint_names(), [f"Stint {created_stint.get_stint_number()} - {driver_acr}"])

    def test_get_driver(self):
        self.assertTrue(True)

    def test_get_set(self):
        self.assertTrue(True)
    
def loading_scenario():
    suite = unittest.TestSuite()
    suite.addTest(TestGlobalHandler('test_load_tyreset'))
    suite.addTest(TestGlobalHandler('test_load_drivers'))
    return suite
    
def interacting_with_stints():
    suite = unittest.TestSuite()
    suite.addTest(TestGlobalHandler('test_add_stint_initial'))
    suite.addTest(TestGlobalHandler('test_add_lap'))
    suite.addTest(TestGlobalHandler('test_add_energy'))
    suite.addTest(TestGlobalHandler('test_add_note'))
    suite.addTest(TestGlobalHandler('test_add_comment'))
    suite.addTest(TestGlobalHandler('test_edit_lap'))
    suite.addTest(TestGlobalHandler('test_remove_lap'))
    suite.addTest(TestGlobalHandler('test_add_pitstop'))
    suite.addTest(TestGlobalHandler('test_add_stint'))
    suite.addTest(TestGlobalHandler('test_generate_stint_names'))
    suite.addTest(TestGlobalHandler('test_get_pit_stop'))
    return suite

def interacting_with_drivers_and_tyres():
    suite = unittest.TestSuite()
    suite.addTest(TestGlobalHandler('test_get_driver'))
    suite.addTest(TestGlobalHandler('test_get_set'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(loading_scenario())
    runner.run(interacting_with_stints())
    # runner.run(interacting_with_drivers_and_tyres())
    runner.run()

