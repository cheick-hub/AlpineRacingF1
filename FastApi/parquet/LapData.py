import json

import numpy as np
import pandas as pd

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.PARQUET import PARQUET


class LapData(PARQUET):
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.LAPDATA
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_run(self, uid: str, variables: list[str],
                     run_data: dict[str, object]) -> dict[str, object]:
        """
        Process the run data for a given UID and variables.

        Args:
            uid (str): The unique identifier for the run.
            variables (list[str]): The list of variables to process.
            run_data (dict[str, object]): The dictionary containing the run data.

        Returns:
            dict[str, object]: The processed run data.

        """
        runvar = {}
        l = np.inf
        for var in variables:
            if not var in run_data:
                continue
            js = json.loads(run_data[var])
            if not js:
                continue

            runvar[var] = np.array([js[i]['0'] for i in js], dtype=float)
            if runvar[var].size < l:
                l = runvar[var].size

        if not runvar:
            return
        for var in runvar:
            runvar[var] = runvar[var][:l]

        runvar['RunUID'] = np.full(l, uid, dtype=np.dtype('U36'))
        runvar['LapCount'] = np.arange(1, l+1, dtype=int)
        return runvar

    def process_data(self, update: bool = False) -> pd.DataFrame:
        dict_runUID = self.create_dict_runUID()
        data = self.cached_read_files(data_type = self.data_type,
                                      dict_runUID = dict_runUID, 
                                      years = self.years,
                                      update = update,
                                      competition = self.competition)

        processed_data = [self._process_run(
            uid, self.variables, data[uid]) for uid in self.run_uid]

        if not any(processed_data):   # que des None ie. aucun return de runvar
            return pd.DataFrame(columns=['RunUID_index', 'LapCount'])

        res = [pd.DataFrame(run) for run in processed_data if run is not None]
        res = pd.concat(res).copy()
        res = self._encode_run_uid(res)
        return res
