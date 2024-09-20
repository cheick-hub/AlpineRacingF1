import pandas as pd

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.PARQUET import PARQUET


class ChannelData(PARQUET):
    """Implement Parquet class.

    Specialized in treating raw Channels. This type of data is not cached.
    Getting the data might be longer than for other cached data types.

    Raises:
        NotImplementedError: If an object of this class is created.
        HTTPException: If the competition passed when creating an object is unknown.
    """
    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.CHANNEL
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_var(self, var: str, data: dict[str, pd.DataFrame], 
                     dict_runUID: dict[str, list[str]]) -> list[pd.DataFrame]:
        """Process data from one var.

        The data is split by variables to be processed one by one.

        Args:
            var: The variable processed for this call.
            data: Channels data retreived either from the parquet files or the cache.
        """
        var_data = []
        for u in dict_runUID:
            df = pd.DataFrame(data[u][var])
            if df.empty:
                continue

            df['Time'] = df['Time'].astype(int)/1e3
            df['RunUID'] = u
            var_data.append(df)
        return var_data

    def process_data(self) -> dict[str, pd.DataFrame]:
        """Process the data for a ChannelData instance.

        Gets the data from the parquet files.
        Regroups data in a dict where keys are variables and values are pd.DataFrame. 

        Return:
            A dict with the channels names as keys and a pd.DataFrame with the channels data.

        Raises:
            HTTPException: If there is a different number of runUIDs and Years.
        """
        dict_runUID = self.create_dict_runUID()

        data = self.read_files(data_type=self.data_type, dict_runUID=dict_runUID,
                               years=self.years)

        res = {var: self._process_var(var, data, dict_runUID) for var in self.variables}
        if not any(res.values()):
            return dict()
        res = {var: pd.concat(l, axis='index', ignore_index=True).copy()
               for var, l in res.items() if l}
        for var, df in res.items():
            res.update({var: self._encode_run_uid(df)})
        return res
