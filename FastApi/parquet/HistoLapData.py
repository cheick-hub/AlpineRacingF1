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


class HistoLapData(PARQUET):
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.HISTOLAPDATA
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_variable(self, var: str, data: dict[str, pd.DataFrame], 
                     dict_runUID: dict[str, list[str]],
                     agg_requested: bool) -> dict[str, np.ndarray]:
        """
        Process a variable from the given data.

        Args:
            var (str): The variable name.
            data (dict[str, object]): The data dictionary.

        Returns:
            dict[str, object]: The processed variable data.

        """
        t0 = time.time()
        var_data = {}
        data_len = 0
        has_data = []
        max_lap_number = 0
        for u in dict_runUID:
            if not var in data[u]:
                continue

            js = json.loads(data[u][var])

            if not js:
                continue
            has_data.append(u)
            lap_count = len(js.keys())
            if lap_count > max_lap_number:
                max_lap_number = lap_count

            if not var_data:  # une variable à toujours le même axis, pas besoin de le regarder à chaque fois
                (x_left, x_right) = self._create_interval(
                    data, u, var + '_xAxis')

                data_len = len(x_left)
                var_data['RunUID'] = np.empty(0, dtype=np.dtype('U36'))

            var_data['RunUID'] = np.append(
                var_data['RunUID'], np.full(data_len, u, dtype=np.dtype('U36')))
            
            for i in range(1, max_lap_number + 1):
                lap_name = 'Lap' + str(i)
                if lap_name in js:
                    if i not in var_data:   # first time seeing this lap number
                        nb_runs_before = len(has_data) - 1 # remove current run
                        time_repeat = nb_runs_before * data_len
                        var_data[i] = np.append(
                            np.full(time_repeat, np.nan, dtype=float),
                            list(js[lap_name].values()))
                    else:
                        var_data[i] = np.append(var_data[i], np.array(
                            list(js[lap_name].values()), dtype=float))
                else:  # lap number not in the current run
                    var_data[i] = np.append(var_data[i],
                                        np.full(data_len, np.nan, dtype=float))
        if var_data:
            times_to_add = len(has_data)

            if not agg_requested:
                uid_list = [u_ for u_ in has_data for _ in range(data_len)]
                var_data['RunUID'] = np.array(uid_list, dtype=np.dtype('U36'))

            var_data['Left'] = np.tile(x_left, times_to_add)
            var_data['Right'] = np.tile(x_right, times_to_add)

            # for key, el in var_data.items():
            #     diff = max_len - el.size
            #     """ Une colonne par tour : complète par des NaN si 
            #     un run a moins de tours qu'un autre, pour la création de la df
            #     """
            #     if diff > 0:
            #         var_data[key] = np.append(el, np.repeat(np.nan, diff))

        t1 = time.time()
        limit, duration = 0.5, t1-t0
        if  duration > limit:
            logger.warning(f"""Processing {var} took more than {limit}s """
                            f"""[{duration:.3f}s]""")

        return var_data

    def process_data(self, update: bool,
                     agg: list[CatanaAggregationEnum]|CatanaAggregationEnum) -> dict[str, pd.DataFrame]:
        """
        Process the data and return a dictionary of pandas DataFrames.

        Args:
            update (bool, optional): Flag indicating whether to update the data. Defaults to False.

        Returns:
            dict[str, pd.DataFrame]: A dictionary where the keys are variable names and the values are pandas DataFrames.

        Raises:
            None

        Example usage:
            histo_lap_data = HistoLapData()
            data = histo_lap_data.process_data(update=True)
        """
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
        agg_names = [a.value for a in agg]

        
        res = {var: self._process_variable(var, data, dict_runUID, agg_requested)
               for var in self.variables}
        res = {var: pd.DataFrame(res[var]) for var in res}
        for var, df in res.items():
            if df.empty:
                continue
            
            if agg_requested:
                laps = df.columns[1:-2]
                for lap in laps:   # makes agg faster
                    df[lap] = df[lap].astype(float)
                agg_dict = {lap_num: agg_names for lap_num in laps}
                agg_dict['Right'] = 'first'
                df = self._aggregate_df(df, by=['Left'], agg_=agg_dict)
                for a in agg_names:
                    tmp = df[a]
                    df = df.drop(columns=a)
                    df[a] = tmp.aggregate(a, axis='columns')

            else:
                df = self._encode_run_uid(df)
            res.update({var: df})
        return res
