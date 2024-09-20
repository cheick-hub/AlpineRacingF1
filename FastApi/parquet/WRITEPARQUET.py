import os
import pandas as pd

from parquet.PARQUET import PARQUET


class WRITEPARQUET(PARQUET):
    def __init__(self, competition, variables, run_uid, years, data_type):
        super().__init__(competition, variables, run_uid, years, data_type)

    def write_file(self, to_write: tuple[str, pd.DataFrame]):
        path = to_write[0]
        data = to_write[1]
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        data.to_parquet(path, index=False)