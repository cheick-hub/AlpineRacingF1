import unittest
import datetime
from src.Driver import Driver, DriverCategory

class TestDriver(unittest.TestCase):

    def setUp(self):
        self.first_name = "Cheick"
        self.last_name = "Fofana"
        self.acr = "CFO" 
        self.driver = Driver(self.first_name, self.last_name, self.acr, DriverCategory.Gold)

    def test_init_time_tracker(self):
        self.driver.init_time_tracker()
        self.assertEqual(len(self.driver.driving_time_per_stint), 1)
        self.assertEqual(len(self.driver.driving_time_per_stint[0]), 0)

    def test_add_driving_time_append(self):
        driving_time = datetime.timedelta(minutes=30)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time, update=False, index=None)
        self.assertEqual(len(self.driver.driving_time_per_stint[0]), 1)
        self.assertEqual(self.driver.driving_time_per_stint[0][0], driving_time)

    def test_add_driving_time_update(self):
        driving_time = datetime.timedelta(minutes=30)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time, update=False, index=None)
        updated_driving_time = datetime.timedelta(minutes=45)
        self.driver.add_driving_time(updated_driving_time, update=True, index=0)
        self.assertEqual(len(self.driver.driving_time_per_stint[0]), 1)
        self.assertEqual(self.driver.driving_time_per_stint[0][0], updated_driving_time)

    def test_get_driving_time(self):
        driving_time = datetime.timedelta(minutes=30)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time, update=False, index=None)
        self.assertEqual(self.driver.get_driving_time(), [[driving_time]])

    def test_get_total_driving_time(self):
        driving_time_1 = datetime.timedelta(minutes=30)
        driving_time_2 = datetime.timedelta(minutes=45)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time_1, update=False, index=None)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time_2, update=False, index=None)
        total_driving_time = driving_time_1 + driving_time_2
        self.assertEqual(self.driver.get_total_driving_time(), total_driving_time)

    def test_get_total_driving_time_per_stint(self):
        driving_time_1 = datetime.timedelta(minutes=30)
        driving_time_2 = datetime.timedelta(minutes=45)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time_1, update=False, index=None)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time_2, update=False, index=None)
        self.assertEqual(self.driver.get_total_driving_time(per_stint=True), [driving_time_1, driving_time_2])

    def test_get_driving_time_for_stint_i(self):
        driving_time_1 = datetime.timedelta(minutes=30)
        driving_time_2 = datetime.timedelta(minutes=45)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time_1, update=False, index=None)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time_2, update=False, index=None)
        self.assertEqual(self.driver.get_driving_time_for_stint_i(0), driving_time_1)
        self.assertEqual(self.driver.get_driving_time_for_stint_i(1), driving_time_2)

    def test_get_acronym(self):
        self.assertEqual(self.driver.get_acronym(), self.acr)

    def test_get_first_name(self):
        self.assertEqual(self.driver.get_first_name(), self.first_name)

    def test_get_last_name(self):
        self.assertEqual(self.driver.get_last_name(), self.last_name)

    def test_get_fullname(self):
        self.assertEqual(self.driver.get_fullname(), f"{self.first_name} {self.last_name}")

    def test_get_category(self):
        self.assertEqual(self.driver.get_category(), DriverCategory.Gold.value)

    def test_increment_number_of_stint(self):
        self.driver.increment_number_of_stint()
        self.assertEqual(self.driver.get_number_of_stint(), 1)

    def test_add_stint_start_time(self):
        start_time = datetime.time(9, 0)
        self.driver.add_stint_start_time(start_time)
        self.assertEqual(len(self.driver.stints_start_time), 1)
        self.assertEqual(self.driver.stints_start_time[0], start_time)

    def test_remove_driving_time(self):
        driving_time = datetime.timedelta(minutes=30)
        self.driver.init_time_tracker()
        self.driver.add_driving_time(driving_time, update=False, index=None)
        self.driver.remove_driving_time(0)
        self.assertEqual(len(self.driver.driving_time_per_stint[0]), 0)

if __name__ == '__main__':
    unittest.main()