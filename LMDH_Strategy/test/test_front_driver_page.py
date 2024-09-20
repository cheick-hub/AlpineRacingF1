import os
import unittest
import pandas as pd
from streamlit.testing.v1 import AppTest

from src.GlobalHandler import GlobalHandler 

DATA_PATH = "test/data"
DRIVER_PATH = DATA_PATH + "/drivers"

if not os.path.exists(DATA_PATH): os.makedirs(DATA_PATH)
if not os.path.exists(DRIVER_PATH): os.makedirs(DRIVER_PATH)

class TestFrontDriverPage(unittest.TestCase):

    def setUp(self):
        self.driver_path = DRIVER_PATH
        self.global_handler = GlobalHandler()
        self.global_handler.add_driver_path(self.driver_path)

        self.app = AppTest.from_file("front/pages/1_Drivers.py")
        self.app.session_state["orchester"] = self.global_handler
        self.app.run()
        assert not self.app.exception

        self.params = {"f_name": "Cheick", "l_name": "Fofana", "acr": "CFO", "cat": "Gold"} 
        assert self.create_driver(**self.params)


    def tearDown(self):
        for file in os.listdir(self.driver_path):
            os.remove(os.path.join(self.driver_path, file))

    def create_driver(self, f_name, l_name, acr, cat):
        self.app.text_input(key="first_name_input").input(f_name).run()
        self.app.text_input(key="last_name_input").input(l_name).run()
        self.app.text_input(key="acronyme_input").input(acr).run()
        self.app.selectbox(key="category_input").select(cat).run()
        self.app.button(key='FormSubmitter:from_add_driver-Add Driver').click().run()
        assert not self.app.exception
        return True

    def test_add_driver_form(self):
        created_driver = self.global_handler.get_drivers()[self.params["acr"]]
        self.assertEqual(created_driver.get_first_name(), self.params["f_name"])
        self.assertEqual(created_driver.get_last_name(), self.params["l_name"])
        self.assertEqual(created_driver.get_acronym(), self.params["acr"])
        self.assertEqual(created_driver.get_category(), self.params["cat"])

    def test_added_driver_info_visible(self):
        selectbox_ = self.app.selectbox(key="select_driver")
        options = selectbox_.options
        selectbox_.select(self.params["acr"]).run()
        assert not self.app.exception
        markdown_contents = [mk_.value for mk_ in self.app.markdown]
        joined_contents = " ".join(markdown_contents)
        self.assertTrue(self.params["acr"] in options)
        for content in [self.params["f_name"], self.params["l_name"], self.params["acr"], self.params["cat"]]:
            self.assertTrue(content in joined_contents)

    def test_added_driver_in_list(self):
        drivers_list : pd.DataFrame = self.app.dataframe[0].value
        self.assertEqual(drivers_list.shape[0], 1)
        self.assertEqual(drivers_list.iloc[0, 0], self.params["f_name"])
        self.assertEqual(drivers_list.iloc[0, 1], self.params["l_name"])
        self.assertEqual(drivers_list.iloc[0, 2], self.params["acr"])
        self.assertEqual(drivers_list.iloc[0, 3], self.params["cat"])


        