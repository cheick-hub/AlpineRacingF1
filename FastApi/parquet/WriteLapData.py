import json

import numpy as np
import pandas as pd

from multiprocessing import Pool

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.WRITEPARQUET import WRITEPARQUET


class WriteLapData(WRITEPARQUET):
    def __init__(self, competition, variables, run_uid, years, to_add, lapNb):
        if competition == 'F1':
            data_type = CatanaDataTypeEnum.LAPDATA
        super().__init__(competition, variables, run_uid, years, data_type)
        self.lapdata_to_add = to_add
        self.lapNb = lapNb

    def _add_none_missing_value(self, df: pd.DataFrame) -> pd.DataFrame:
        
        lapNb_already_written = df.columns.size
        to_insert = {
            'Lap'+str(i) : None
            for i in range(lapNb_already_written+1, self.lapNb)
        }
        if to_insert:
            return pd.concat([df, pd.DataFrame(to_insert, index=[0])],
                             axis=1)
        
        return df


    def process_data(self) -> list[tuple[str, pd.DataFrame]]:
        dict_runUID = self.create_dict_runUID()

        already_written = self.read_files(data_type=self.data_type,
                                          dict_runUID=dict_runUID,
                                          years=self.years)
        
        path_data = []
        for (i, run_uid), var in zip(enumerate(dict_runUID), self.variables) :
            already_written_run = already_written[run_uid]
            if not var in already_written_run:
                # create new dataframe and add the current lap
                df = self._add_none_missing_value(pd.DataFrame())
            else: 
                df = already_written[run_uid][var]
                df = self._add_none_missing_value(df)
                
            lap_name = 'Lap'+str(self.lapNb)
            if lap_name in df.columns and df[lap_name].iloc[0]:
                continue   # lapdata already written for this variable
            df[lap_name] = self.lapdata_to_add[var]

            # while testing
            # path = self.parquet_path + str(self.years[i])
            path = '/mnt/c/Users/lacou/OneDrive - Whiteways/Desktop/test_write_parquet/'
            path += '/' + run_uid
            path += '/' +  self.folder_datatype[self.data_type]
            path += '/' + var + '.parquet'
            
            path_data.append((path, df))

        return path_data

    def write_parquet(self):
        path_data = self.process_data()
        num_processes = min(6, len(path_data))   # arbitraire

        with Pool(num_processes) as pool:
            pool.map(self.write_file, path_data)

        print(f'Updated {len(path_data)} parquet files')