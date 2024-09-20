import unittest
import datetime
import numpy as np
from src.utilities import format_time, sum_times, cumsum_times, InputParser

class TestUtilities(unittest.TestCase):

    def test_format_time(self):
        laptime = datetime.timedelta(minutes=2, seconds=34, milliseconds=560)
        self.assertEqual(format_time(laptime), '02:34.00')
        self.assertEqual(format_time(laptime, show_milliseconds=True), '02:34.56')
        self.assertEqual(format_time(laptime, show_hour=True), '0:02:34')
        self.assertEqual(format_time(laptime, show_hour=True, show_milliseconds=True), '02:34.56')

    def test_sum_times(self):
        times = [datetime.timedelta(minutes=1, seconds=30),
                 datetime.timedelta(minutes=2, seconds=15),
                 datetime.timedelta(minutes=0, seconds=45)]
        self.assertEqual(sum_times(times), datetime.timedelta(minutes=4, seconds=30))

    def test_cumsum_times(self):
        times = [datetime.timedelta(minutes=1, seconds=30),
                 datetime.timedelta(minutes=2, seconds=15),
                 datetime.timedelta(minutes=0, seconds=45)]
        self.assertTrue(np.all(cumsum_times(times) == np.array([datetime.timedelta(minutes=1, seconds=30),
                                                        datetime.timedelta(minutes=3, seconds=45),
                                                        datetime.timedelta(minutes=4, seconds=30)])))
    def test_is_valid_hour(self):
        self.assertTrue(InputParser.is_valid_hour('12'))
        self.assertFalse(InputParser.is_valid_hour('24'))
        self.assertFalse(InputParser.is_valid_hour('a'))

    def test_if_valid_minute_or_second(self):
        self.assertTrue(InputParser.if_valid_minute_or_second('30'))
        self.assertFalse(InputParser.if_valid_minute_or_second('60'))
        self.assertFalse(InputParser.if_valid_minute_or_second('a'))

    def test_is_valid_hundredth(self):
        self.assertTrue(InputParser.is_valid_hundredth('50'))
        self.assertFalse(InputParser.is_valid_hundredth('100'))
        self.assertFalse(InputParser.is_valid_hundredth('a'))

    def test_parse_lap_input(self):
        lap_input = '14578963'
        expected_output = (datetime.timedelta(minutes=1, seconds=45, milliseconds=780), 9.63)
        self.assertEqual(InputParser.parse_lap_input(lap_input), expected_output)

    def test_parse_float_input(self):
        float_input = '3.14'
        expected_output = 3.14
        self.assertEqual(InputParser.parse_float_input(float_input), expected_output)

    def test_parse_pitstop_time(self):
        pit_stop_time = '01.30'
        expected_output = datetime.timedelta(minutes=1, seconds=30)
        self.assertEqual(InputParser.parse_pitstop_time(pit_stop_time), expected_output)

if __name__ == '__main__':
    unittest.main()