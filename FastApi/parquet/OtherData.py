import json

import numpy as np
import pandas as pd

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.PARQUET import PARQUET


class OtherData(PARQUET):
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.OTHER
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_variable(self, var: str, data: dict[str, pd.DataFrame], 
                     dict_runUID: dict[str, list[str]]) -> dict[str, np.ndarray]:
        """
        Process the variable data for a given variable and return a dictionary containing the processed data.

        Args:
            var (str): The name of the variable to process.
            data (dict): A dictionary containing the data for each run.

        Returns:
            dict: A dictionary containing the processed variable data. The dictionary has two keys:
                - The variable name (var) as the key, and the processed data as the value.
                - 'RunUID' as the key, and an array of run IDs corresponding to the processed data as the value.
        """
        var_data = {}
        for u in dict_runUID:
            if not var in data[u]:
                continue
            js = json.loads(data[u][var])
            if not js:
                continue
            if not var_data:
                # initialisation des np.arrays
                var_data[var] = np.array(list(js['Run'].values()), dtype=float)
                var_data['RunUID'] = np.full(
                    var_data[var].size, u, dtype=np.dtype('U36'))
                continue

            var_run_data = np.array(list(js['Run'].values()), dtype=float)
            data_len = var_run_data.size
            var_data[var] = np.append(var_data[var], var_run_data)
            var_data['RunUID'] = np.append(
                var_data['RunUID'], np.full(data_len, u, dtype=np.dtype('U36')))
        return var_data

    def process_data(self, update: bool) -> dict[str, pd.DataFrame]:
            """
            Process the data based on the specified variables and return a dictionary of DataFrames.

            Args:
                update (bool): Flag indicating whether to update the cached data or not.

            Returns:
                dict[str, pd.DataFrame]: A dictionary containing processed data for each variable.
            """
            dict_runUID = self.create_dict_runUID()

            data = self.cached_read_files(data_type = self.data_type,
                                          dict_runUID = dict_runUID, 
                                          years = self.years,
                                          update = update,
                                          competition = self.competition)

            res = {var: self._process_variable(var, data, dict_runUID)
                   for var in self.variables}

            if not res:
                return pd.DataFrame(columns=['RunUID_index', 'LapCount'])

            res = {var: pd.DataFrame(res[var]) for var in res}
            for var, df in res.items():
                res.update({var: self._encode_run_uid(df)})
            return res
