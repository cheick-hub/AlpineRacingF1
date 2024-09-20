import unittest
from unittest.mock import patch
import os
print(os.getcwd())

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.LapData import LapData
from parquet.PARQUET import PARQUET


class TestLapData(unittest.TestCase):
    @patch('LapData.cached_read_files')
    def test_lapdata(self, mock_read_file):
        mock_read_file.return_value = []

        ld = LapData('F1', ['nEngine_MAX'], ['uid'], [2024])
        res = ld.process_data()
        self.assertEqual(res, [])
        # data = mock_read_file.return_value

if __name__ == "__main__":
    unittest.main()
