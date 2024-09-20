from itertools import chain, product
import json
import logging
import time

import numpy as np
import pandas as pd

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.CatanaAggregationEnum import CatanaAggregationEnum
from parquet.PARQUET import PARQUET

logger = logging.getLogger('main_log')

class Histo2DData(PARQUET):
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.HISTO2D
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_variable(self, var: str, data: dict[str, pd.DataFrame],
                     dict_runUID: dict[str, list[str]],
                     to_agg: bool) -> dict[str, np.ndarray]:
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
        has_data = []
        for u in dict_runUID:
            if not var in data[u]:
                continue

            js = json.loads(data[u][var])

            if not js:
                continue

            has_data.append(u)
            if not var_data:
                (x_left, x_right) = self._create_interval(
                    data, u, var + '_xAxis')
                (y_left, y_right) = self._create_interval(
                    data, u, var + '_yAxis')
                data_len = len(x_left) * len(y_left)
                var_data[var] = np.empty(0, dtype=float)

            var_run_data = np.array(list(js['Run'].values()), dtype=float)
            var_data[var] = np.append(var_data[var], var_run_data)

        if var_data:
            if not to_agg:
                uid_list = [u_ for u_ in has_data for _ in range(data_len)]
                var_data['RunUID'] = np.array(uid_list, dtype=np.dtype('U36'))

            var_size = var_data[var].size
            times_repeat = var_size // data_len
            grid = np.array(
                list(product(zip(y_left, y_right), zip(x_left, x_right))),
                dtype=float)
            var_data['y_Left'] = np.tile(grid[:, 0, 0], times_repeat)
            var_data['y_Right'] = np.tile(grid[:, 0, 1], times_repeat)
            var_data['x_Left'] = np.tile(grid[:, 1, 0], times_repeat)
            var_data['x_Right'] = np.tile(grid[:, 1, 1], times_repeat)

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
            (var, var + '_xAxis', var + '_yAxis') for var in self.variables))
        dict_runUID = self.create_dict_runUID(variables=variables_with_axes)

        data = self.cached_read_files(data_type=self.data_type,
                                      dict_runUID=dict_runUID,
                                      years=self.years,
                                      update=update,
                                      competition=self.competition)
        
        res = {}
        if not isinstance(agg, list):
            agg = [agg]
        agg_requested = (CatanaAggregationEnum.NONE not in agg)
        for var in self.variables:
            t0 = time.time()
            res[var] = self._process_variable(var, data,
                                              dict_runUID, agg_requested)
            t1 = time.time()
            limit, duration = 1, t1-t0
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
                dict_agg = {var: [a.value for a in agg],
                            'y_Right': 'first', 'x_Right': 'first'}
                df = self._aggregate_df(df, by = ['y_Left', 'x_Left'],
                                                agg_=dict_agg)
            else:
                df = self._encode_run_uid(df)

            res.update({var: df})
        return res
