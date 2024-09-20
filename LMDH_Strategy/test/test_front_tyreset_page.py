import os
import unittest
import pandas as pd
from streamlit.testing.v1 import AppTest

from src.GlobalHandler import GlobalHandler 

DATA_PATH = "test/data"
TYRE_PATH = DATA_PATH + "/tyres"

if not os.path.exists(DATA_PATH): os.makedirs(DATA_PATH)
if not os.path.exists(TYRE_PATH): os.makedirs(TYRE_PATH)

class TestFrontTyrePage(unittest.TestCase):

    def setUp(self):
        self.tyre_path = TYRE_PATH
        self.global_handler = GlobalHandler()
        self.global_handler.add_tyre_path(self.tyre_path)
        
        self.app = AppTest.from_file("front/pages/2_Tyres.py")
        self.app.session_state["orchester"] = self.global_handler
        self.app.run()
        assert not self.app.exception

        self.params = {
            "set_name" : "TS1",
            "mileage" : 10,
            "compound" : "Soft"
        }
        self.add_tyreset(**self.params)

    def tearDown(self):
         for file in os.listdir(self.tyre_path):
             os.remove(os.path.join(self.tyre_path, file))

    def add_tyreset(self, set_name, mileage, compound):
        self.app.text_input(key="set_input").input(set_name).run()
        self.app.number_input(key="mileage_input").set_value(mileage).run()
        self.app.selectbox(key="compound_input").select(compound).run()
        # self.app.number_input(key="front_left_rim_input").set_value(FL).run()
        # self.app.number_input(key="rear_left_rim_input").set_value(RL).run()
        # self.app.number_input(key="front_right_rim_input").set_value(FR).run()
        # self.app.number_input(key="rear_right_rim_input").set_value(RR).run()
        self.app.button(key='FormSubmitter:from_add_tyre-Add Set').click().run()
        assert not self.app.exception, "Exception raised while adding tyre"

    def test_add_tyre_form(self):
        created_tyre = self.global_handler.get_sets()[self.params["set_name"]]
        self.assertEqual(created_tyre.get_set_name(), self.params["set_name"])
        self.assertEqual(created_tyre.get_mileage(), self.params["mileage"])
        self.assertEqual(created_tyre.get_compound(), self.params["compound"])

    def test_added_tyre_visible_in_tyre_list(self):
        tyres_list : pd.DataFrame = self.app.dataframe[0].value
        self.assertEqual(tyres_list.shape[0], 1)
        self.assertEqual(tyres_list.iloc[0, 0], self.params["set_name"])
        self.assertEqual(tyres_list.iloc[0, 1], self.params["mileage"])
        self.assertEqual(tyres_list.iloc[0, 2], self.params["compound"])
        