import os
import unittest
import datetime
import pandas as pd
from streamlit.testing.v1 import AppTest

from src.Compound import Compound
from src.Driver import DriverCategory
from src.GlobalHandler import GlobalHandler 

DATA_PATH = "test/data"
TYRE_PATH = DATA_PATH + "/tyres"
DRIVER_PATH = DATA_PATH + "/drivers"
RACE_DATA_PATH = DATA_PATH + "/race_data"

if not os.path.exists(DATA_PATH): os.makedirs(DATA_PATH)
if not os.path.exists(TYRE_PATH): os.makedirs(TYRE_PATH)
if not os.path.exists(DRIVER_PATH): os.makedirs(DRIVER_PATH)

class TestFrontStintPage(unittest.TestCase):
    def setUp(self):
        self.tyre_path = TYRE_PATH
        self.driver_path = DRIVER_PATH
        self.global_handler = GlobalHandler()
        self.start_time = datetime.time(11, 0)

        self.app = AppTest.from_file("front/pages/3_Stint.py")
        self.initialize_global_handler()
        self.initialize_app()
        self.app.session_state["orchester"] = self.global_handler
        self.app.run()
        assert not self.app.exception

        self.init_params = {
            "cons_form_input" : 0.5,
            "nb_foramtion_laps" : 2,
            "cons_lap2grid_input": 0.5
        }

    def tearDown(self):
        for file in os.listdir(self.tyre_path):
            os.remove(os.path.join(self.tyre_path, file))
        for file in os.listdir(self.driver_path):
            os.remove(os.path.join(self.driver_path, file))

    def initialize_app(self):
        self.app.session_state["car_number"] = "#35"
        self.app.session_state["venue"] = "Monza"
        self.app.session_state["race_duration"] = 6
        self.app.session_state['min_driving_time'] = datetime.time(1,0)
        self.app.session_state['max_driving_time'] = datetime.time(6,00)
        self.app.session_state['venue'] = "Kamsar"
        self.app.session_state["event_race_start_time"] = datetime.time(11, 0)
        self.app.session_state['capacity'] = 70.0
        self.app.session_state['track_length'] = 4.55
        self.app.session_state['fuel_density'] = 1.0
        self.app.session_state['stint_energy_sim'] = 900
        self.app.session_state['lap_energy_sim'] = 30
        self.app.session_state['lap_alarm'] = 2
        self.app.session_state['min_oil_level'] = 3
        self.app.session_state['max_oil_level'] = 5
        self.app.session_state['data_saved'] = True
        self.app.session_state['data_path'] = RACE_DATA_PATH
        
    def initialize_global_handler(self):
        self.global_handler.add_tyre_path(self.tyre_path)
        self.global_handler.add_driver_path(self.driver_path)
        self.create_drivers()
        self.create_sets()
        
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

    def test_venue_name_visible(self):
        self.assertEqual(self.app.subheader[0].value, self.app.session_state["venue"])

    def create_stint(self, driver_acr : str, set_name : str, first_stint : bool = False, init_params : dict = None):
        self.app.selectbox[0].select('Create a new stint').run()
        self.app.selectbox(key="driver_choice").select(driver_acr).run()
        self.app.selectbox(key="tyreset_input").select(set_name).run()
        # self.app.time_input(key="race_starttime_input").set_value("11:00").run()
        
        if first_stint:
            assert init_params is not None, "init_params must be provided for first stint"
            self.app.number_input(key="cons_lap2grid_input").set_value(init_params["cons_lap2grid_input"]).run()
            self.app.number_input(key="formation_lap_conso_input").set_value(init_params["cons_form_input"]).run()
            self.app.number_input(key="nb_formation_lap_input").set_value(init_params["nb_foramtion_laps"]).run()
        else:
            self.app.text_input(key="pit_stop_time_input").set_value("1.00").run()
            self.app.number_input(key="fuel_pit_input").set_value(10).run()
            
        self.app.button[0].click().run()
        assert not self.app.exception, f"[first_stint={first_stint}] Could not create stint for driver {driver_acr} and set {set_name}"

    def click_on_create_stint(self):
        self.app[1].selectbox[0].select('Create a new stint').run()
        assert not self.app.exception, "Could not click on create new stint"

    def reset_param(self):
        self.app.session_state['driver_choice'] = "MFO"
        self.app.session_state['tyreset_input'] = "TS2"
        self.app.session_state["cons_lap2grid_input"] = 0.5
        self.app.session_state["formation_lap_conso_input"] = 0.5
        self.app.session_state["nb_formation_lap_input"] = 2
        if self.app.session_state["orchester"].Stints.__len__() > 1:
           self.app.session_state["pit_stop_time_input"] = "1.00"
           self.app.session_state["fuel_pit_input"] = 10
        self.app.session_state["race_starttime_input"] = datetime.time(11,00)
        
    def add_laps(self, stint_id : int, inputs : list[str]) -> None:
        for input_ in inputs:
            self.app.session_state["orchester"].add_lap(stint_id,input_)
        self.reset_param()
        self.app.run()
        assert not self.app.exception, "Could not add laps"

    def test_create_first_stint(self):
        driver_name = "CFO"
        driver_set = "TS1"
        self.create_stint(driver_acr=driver_name, set_name=driver_set, first_stint=True, init_params=self.init_params)
        
        special_laps = self.global_handler.get_nth_stint(0).get_special_laps()
        laps = self.global_handler.get_nth_stint(0).get_laps()
        self.assertEqual(self.app.selectbox(key="stint_driver_input").value, driver_name)
        self.assertEqual(set(self.app.dataframe[0].value.columns),set(special_laps.columns))
        self.assertEqual(set(self.app.dataframe[1].value.columns), set(laps.columns))
        self.assertEqual(special_laps['Fuel Used (kg/lap)'][0], self.init_params["cons_lap2grid_input"])
        self.assertEqual(special_laps['Fuel Used (kg/lap)'][1], self.init_params["cons_form_input"])
        self.assertEqual(len(self.app.dataframe), 2)

    def test_create_nth_stint(self):
        st_1_driver = "CFO"
        st_1_set = "TS1"
        st_2_driver = "MFO"
        st_2_set = "TS2"

        self.create_stint(driver_acr=st_1_driver, set_name=st_1_set, first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["14578963", "14578963"])
        self.click_on_create_stint()
        self.create_stint(driver_acr=st_2_driver, set_name=st_2_set, first_stint=False)

        first_stint = self.global_handler.get_nth_stint(0)
        created_stint = self.global_handler.get_nth_stint(1)
        stint_options = self.global_handler.generate_stint_names()
        
        self.assertEqual(self.app.selectbox(key="stint_driver_input").value, st_2_driver)
        self.assertEqual(set(self.app[1].selectbox[0].options), set(["Create a new stint", "-"] + stint_options))
        self.assertEqual(len(self.app.dataframe), 2)
        self.assertEqual(first_stint.get_driver().get_acronym(), st_1_driver)
        self.assertEqual(first_stint.get_tyreset().get_set_name(), st_1_set)
        self.assertEqual(created_stint.get_driver().get_acronym(), st_2_driver)
        self.assertEqual(created_stint.get_tyreset().get_set_name(), st_2_set)
    
    def test_stint_summary_displayed(self):
        self.create_stint(driver_acr="CFO", set_name="TS1", first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["14578963", "14578963"])

        columns =  ["Driver", "Fuel added", "Fuel margin", "Tire", "Stint laps", "Race Laps", "Fuel", "InLap", "Stint Time", "Race Time"]
        front_columns = self.app.dataframe[0].columns
        self.assertEqual(len(self.app.dataframe), 3)
        for col in columns:
            self.assertTrue(col in front_columns)
        self.assertEqual(len(self.app.dataframe), 3)
        self.assertTrue(self.global_handler.get_nth_stint(0).get_driver().get_acronym() in front_columns)
        
    def test_fatest_lap_computed_updated_all_cases(self):
        self.create_stint(driver_acr="CFO", set_name="TS1", first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["10000963", "20000963", "30000963"])

        # verify that the fastest lap is updated
        self.assertEqual(self.app.selectbox(key="fastest_lap_display").value, '01:00.00')

        # add note to set the fastest lap to OUTLAP
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="OUT")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="fastest_lap_display").value, '02:00.00')

        # remove the note to set the fastest lap
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="fastest_lap_display").value, '01:00.00')
        
        # add a lap that is faster than the current fastest lap
        self.add_laps(stint_id=1, inputs=["02000963"])
        self.app.run()
        self.assertEqual(self.app.selectbox(key="fastest_lap_display").value, '00:20.00')

        # test the average lap when a lap is removed
        self.global_handler.remove_lap(stint_nb=1, lap_idx=self.global_handler.get_nth_stint(0).get_nb_laps()-1)
        self.app.run()
        self.assertEqual(self.app.selectbox(key="fastest_lap_display").value, '01:00.00')

        # test the average lap a lap is edited
        self.global_handler.edit_lap(stint_nb=1, edited_index=0, edited_input="03000963")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="fastest_lap_display").value, '00:30.00')
    
    def test_mean_lap_computed_updated_all_cases(self):
        self.create_stint(driver_acr="CFO", set_name="TS1", first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["10000963", "20000963", "30000963"])

        # verify that the average lap is updated
        self.assertEqual(self.app.selectbox(key="average_lap_display").value, '02:00.00')

        # add note to set the average lap to OUTLAP
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="OUT")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_lap_display").value, '02:30.00')

        # remove the note to update the average lap
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_lap_display").value, '02:00.00')

        # add a lap that to how the average lap is updated
        self.add_laps(stint_id=1, inputs=["02000963"])
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_lap_display").value, '01:35.00')

        # test the average lap when a lap is removed
        self.global_handler.remove_lap(stint_nb=1, lap_idx=self.global_handler.get_nth_stint(0).get_nb_laps()-1)
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_lap_display").value, '02:00.00')

        # test the average lap a lap is edited
        self.global_handler.edit_lap(stint_nb=1, edited_index=0, edited_input="40000963")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_lap_display").value, '03:00.00')

    def test_fuel_consumption_computed_updated_all_cases(self):
        self.create_stint(driver_acr="CFO", set_name="TS1", first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["10000100", "20000200", "30000300"])

        # verify that the average consumption is updated
        self.assertEqual(self.app.selectbox(key="average_fuel_display").value, 2.0)

        # add note to set the average consumption to OUTLAP
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="OUT")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_fuel_display").value, 2.5)

        # remove the note to update the average consumption
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_fuel_display").value, 2.0)

        # add a lap that to how the average consumption is updated
        self.add_laps(stint_id=1, inputs=["02000400"])
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_fuel_display").value, 2.5)

        # test the average consumption when a lap is removed
        self.global_handler.remove_lap(stint_nb=1, lap_idx=self.global_handler.get_nth_stint(0).get_nb_laps()-1)
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_fuel_display").value, 2.0)

        # test the average consumption a lap is edited
        self.global_handler.edit_lap(stint_nb=1, edited_index=0, edited_input="40000400")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_fuel_display").value, 3.0)

    def test_energy_consumption_computed_updated_all_cases(self):
        self.create_stint(driver_acr="CFO", set_name="TS1", first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["10000100", "20000200", "30000300"])
        self.global_handler.add_energy(stint_nb=1, lap_idx=0, cons=1)
        self.global_handler.add_energy(stint_nb=1, lap_idx=1, cons=2)
        self.global_handler.add_energy(stint_nb=1, lap_idx=2, cons=3)
        assert self.app.run(), "Could not add energy"

        # verify that the average energy is updated
        self.assertEqual(self.app.selectbox(key="average_energy_display").value, 2.0)

        # add note to set the average energy to OUTLAP
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="OUT")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_energy_display").value, 2.5)

        # remove the note to update the average energy
        self.global_handler.add_note(stint_nb=1, lap_idx=0, note="")
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_energy_display").value, 2.0)

        # add a lap that to how the average energy is updated
        self.add_laps(stint_id=1, inputs=["02000400"])
        self.global_handler.add_energy(stint_nb=1, lap_idx=3, cons=4)
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_energy_display").value, 2.5)

        # test the average energy when a lap is removed
        self.global_handler.remove_lap(stint_nb=1, lap_idx=self.global_handler.get_nth_stint(0).get_nb_laps()-1)
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_energy_display").value, 2.0)

        # test the average energy a lap is edited
        self.global_handler.add_energy(stint_nb=1, lap_idx=0, cons=4)
        self.app.run()
        self.assertEqual(self.app.selectbox(key="average_energy_display").value, 3.0)
    
    def test_verify_stint_summary_display_update(self):
        
        st_1_driver = "CFO"
        st_1_set = "TS1"
        st_2_driver = "MFO"
        st_2_set = "TS2"

        self.create_stint(driver_acr=st_1_driver, set_name=st_1_set, first_stint=True, init_params=self.init_params)
        self.add_laps(stint_id=1, inputs=["10000963", "10000963"])
        self.click_on_create_stint()
        self.create_stint(driver_acr=st_2_driver, set_name=st_2_set, first_stint=False) # pitsop time = 1.00, fuel = 10
        self.add_laps(stint_id=2, inputs=["10000963", "10000963", "10000963"])
        
        stint_recap_dataframe : pd.DataFrame = self.app.dataframe[0].value
        stint_1_driver_acr = self.global_handler.get_nth_stint(0).get_driver().get_acronym()
        stint_2_driver_acr = self.global_handler.get_nth_stint(1).get_driver().get_acronym()
        self.assertEqual(len(self.app.dataframe), 2)
        self.assertTrue(stint_1_driver_acr in stint_recap_dataframe.columns)
        self.assertTrue(stint_2_driver_acr in stint_recap_dataframe.columns)

        self.assertEqual(stint_recap_dataframe[stint_recap_dataframe['Driver'] == stint_1_driver_acr][stint_1_driver_acr].values[0], "0:02:00")
        self.assertEqual(stint_recap_dataframe[stint_recap_dataframe['Driver'] == stint_2_driver_acr][stint_2_driver_acr].values[0], "0:03:00")

        # test the average lap a lap is edited
        self.global_handler.edit_lap(stint_nb=1, edited_index=0, edited_input="20000963")
        self.app.run()
        stint_recap_dataframe : pd.DataFrame = self.app.dataframe[0].value
        self.assertEqual(stint_recap_dataframe[stint_recap_dataframe['Driver'] == stint_1_driver_acr][stint_1_driver_acr].values[0], "0:03:00")
