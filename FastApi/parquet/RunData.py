import json
import pandas as pd

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.PARQUET import PARQUET


class RunData(PARQUET):
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.RUNDATA
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_run(self, uid: str, run_data: dict[str, object]) -> dict[str, object]:
        """
        Process the run data for a given UID.

        Args:
            uid (str): The UID of the run.
            run_data (dict[str, object]): The run data dictionary.

        Returns:
            dict[str, object]: The processed run data dictionary.
        """
        runvar = {}
        for var in self.variables:
            if not var in run_data:
                continue
            js = json.loads(run_data[var])
            if not js:
                continue
            runvar[var] = js['Run']['0']
            
        if not runvar:
            return
        runvar['RunUID'] = uid
        return runvar

    def process_data(self, update: bool) -> pd.DataFrame:
        """
        Process the data for each run UID and return the processed data as a pandas DataFrame.

        Args:
            update (bool): Flag indicating whether to update the data or use cached data.

        Returns:
            pd.DataFrame: Processed data as a pandas DataFrame with columns ['RunUID_index', 'LapCount'].
        """
        dict_runUID = self.create_dict_runUID()
        data = self.cached_read_files(data_type = self.data_type,
                                      dict_runUID = dict_runUID, 
                                      years = self.years,
                                      update = update,
                                      competition = self.competition)

        processed_data = []
        for uid in dict_runUID:
            processed_run = self._process_run(uid, data[uid])
            if processed_run:
                processed_data.append(processed_run)
        
        if not processed_data:   # que des None ie. aucun return de runvar
            return pd.DataFrame(columns=['RunUID_index'])

        res = pd.DataFrame(processed_data)
        res = self._encode_run_uid(res)
        return res
