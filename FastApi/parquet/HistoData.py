from itertools import chain
import json
import logging
import time

import numpy as np
import pandas as pd

from parquet.CatanaAggregationEnum import CatanaAggregationEnum
from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.PARQUET import PARQUET

logger = logging.getLogger('main_log')

class HistoData(PARQUET):
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.HISTO
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_variable(self, var: str, data: dict[str, pd.DataFrame], 
                     dict_runUID: dict[str, list[str]],
                     agg_requested: bool) -> dict[str, np.ndarray]:
        """
        Process the variable data for a given variable.

        Args:
            var (str): The name of the variable.
            data (dict): The data dictionary containing the variable data.

        Returns:
            dict: A dictionary containing the processed variable data.

        """
        var_data = {}
        data_len = 0
        runs_with_data = []
        for u in dict_runUID:
            if not var in data[u]:
                continue

            js = json.loads(data[u][var])

            if not js:
                continue
            
            runs_with_data.append(u)
            if not var_data:  # une variable à toujours le même axis, pas besoin de le regarder à chaque fois
                (x_left, x_right) = self._create_interval(
                    data, u, var + '_xAxis')
                data_len = len(x_left)
                var_data[var] = np.empty(0, dtype=float)
                # var_data['RunUID'] = np.empty(0, dtype=np.dtype('U36'))

            var_run_data = np.array(list(js['Run'].values()))
            var_data[var] = np.append(var_data[var], var_run_data)
            # var_data['RunUID'] = np.append(
            #     var_data['RunUID'], np.full(data_len, u, dtype=np.dtype('U36')))

        if var_data:
            if not agg_requested:
                uids = [u_ for u_ in runs_with_data for _ in range(data_len)]
                var_data['RunUID'] = np.array(uids, dtype=np.dtype('U36'))
            var_size = var_data[var].size
            times_to_add = (var_size // data_len)

            var_data['Left'] = np.tile(x_left, times_to_add)
            var_data['Right'] = np.tile(x_right, times_to_add)

        return var_data

    def process_data(self, update: bool,
                     agg: list[CatanaAggregationEnum]|CatanaAggregationEnum) -> dict[str, pd.DataFrame]:
        """
        Process the data and return a dictionary of pandas DataFrames.

        Args:
            update (bool): Flag indicating whether to update the data or use cached data.

        Returns:
            dict[str, pd.DataFrame]: A dictionary where the keys are variable names and the values are pandas DataFrames.

        """

        if not agg:
            logger.warning(' No aggregation function received, default to sum')
            agg = [CatanaAggregationEnum.SUM]

        variables_with_axes = list(chain.from_iterable(
            (var, var + '_xAxis') for var in self.variables))
        dict_runUID = self.create_dict_runUID(variables=variables_with_axes)

        data = self.cached_read_files(data_type = self.data_type,
                                      dict_runUID = dict_runUID, 
                                      years = self.years,
                                      update = update,
                                      competition = self.competition)

        if not isinstance(agg, list):
            agg = [agg]
        agg_requested = (CatanaAggregationEnum.NONE not in agg)
        res = {}
        for var in self.variables:
            t0 = time.time()
            res[var] = self._process_variable(var, data, dict_runUID,
                                              agg_requested)
            t1 = time.time()
            limit, duration = 0.5, t1-t0
            if  duration > limit:
                logger.warning(f"""Processing {var} took more than {limit}s """
                               f"""[{duration:.3f}s]""")
        
        if not any(res.values()):
            return {}
        
        res = {var: pd.DataFrame(res[var]) for var in res if res[var]}

        for var, df in res.items():
            if df.empty:
                continue
            if agg_requested:
                dict_agg = {var:[a.value for a in agg],
                            'Right': 'first'}
                df = self._aggregate_df(df, by=['Left'], agg_=dict_agg)
            else:
                df = self._encode_run_uid(df)

            res.update({var: df})

        return res
