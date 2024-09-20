import abc
import json
import logging
from multiprocessing import Pool
import os
import platform
import time
import yaml


from fastapi import HTTPException
import pandas as pd

from cache.cache_decorator import rediscache
from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum


# Mount U : sudo mount -t drvfs U: /mnt/u/ (mkdir /mnt/u si existe pas)

logger = logging.getLogger('main_log')

class PARQUET(metaclass=abc.ABCMeta):
    """An abstract class. Superclass of all data_type classes.

    Raises:
        NotImplementedError: if an object of this class is created.
        HTTPException: if the competition passed when creating an object is unknown.
        FileNotFoundError: if the disk with the F1 parquet files is not mounted.    
    """

    def __init__(self, competition, variables, run_uid, years, data_type):
        self.variables = self._check_if_list(variables)
        self.run_uid = self._check_if_list(run_uid)
        self.years = self._check_if_list(years)
        self.competition = competition
        self.data_type = data_type

        with open('parquet/disk_mapping.yml') as file:
            disk_mapping = yaml.safe_load(file)

        os_ = platform.system()
        if os_ not in disk_mapping:
            msg = f' Unknown operating system: {os_}'
            logger.critical(msg + '\n')
            raise HTTPException(400, msg)
        if competition not in disk_mapping[os_]:
            msg = f' Unknown competition: {competition}'
            logger.critical(msg + '\n')
            raise HTTPException(400, msg)

        self.disk = disk_mapping[os_][competition]
        self.parquet_path = self.disk+"OUTILS_DP/CATANA/OFFICIAL_PARQUETS/"
        if self.competition == 'F1' or self.competition == 'F1 LIVE':
            self.parquet_path = self.disk+"OUTILS_DP/CATANA_TEMP/OFFICIAL_PARQUETS/"

        if not os.path.ismount(self.disk):
            msg = f' Check if {self.disk} drive is mounted.'
            logger.critical(msg + '\n')
            raise HTTPException(400, msg)

        self.folder_datatype = {
            CatanaDataTypeEnum.LAPDATA: 'computed_data/lapdata/',
            CatanaDataTypeEnum.HISTO: 'computed_data/histodata/',
            CatanaDataTypeEnum.HISTOLAPDATA: 'computed_data/histolapdata/',
            CatanaDataTypeEnum.OTHER: 'computed_data/otherdata/',
            CatanaDataTypeEnum.RUNDATA: 'computed_data/rundata/',
            CatanaDataTypeEnum.CDCDATA: 'computed_data/cdcdata/',
            CatanaDataTypeEnum.HISTO2D: 'computed_data/histo2ddata/',
            CatanaDataTypeEnum.CHANNEL: 'channels/',
            CatanaDataTypeEnum.METADATA: ''
        }

    @abc.abstractmethod
    def process_data(self, update: bool = False):
        raise NotImplementedError

    @staticmethod
    def read_parquet_file(file: str) -> pd.DataFrame:
        """Retrive data from a parquet file.

        Args:
            file: The parquet file path.

        Returns:
            A dataframe containing the data from the parquet.
        """
        uid = file[0]
        var = file[1]
        path = file[2]
        if os.path.exists(path):
            df = pd.read_parquet(path)
            return uid, var, df
        
        return uid, var, pd.DataFrame()

    @rediscache
    def cached_read_files(self, data_type: CatanaDataTypeEnum,
                          dict_runUID: dict[str, list[str]],
                          years: list[int],
                          update: bool,
                          competition: str) -> dict[str, dict[str, list]]:
        """Same function as read_file. Called if the data needs to be cached.

        Args:
            data_type: CatanaDataTypeEnum for the corresponding data type
            (ie. LapData, RunData...)
            dict_runUID: a dict where the keys runUIDs and the values are
            a list of variables the data needs to be retrive from the files.
            years: a list contining the years of the runUIDs.
            Needs to be the same length as the keys of dict_runUID.
            update: a boolean indicating if the cache needs to update
            the cached data.

        Returns:
            A dict where the keys are runUIDs and the values is a dict
            with keys and the values are JSON of the DataFrame from the
            read files.
        """
        return self.read_files(data_type=data_type, dict_runUID=dict_runUID,
                               years=years, to_json=True)

    def read_files(self, data_type: CatanaDataTypeEnum | str,
                   dict_runUID: dict[str, list[str]],
                   years: list[int],
                   to_json: bool = False) -> dict[str, dict[str, list]]:
        """Function to read all the parquet files requested. 

        Args:
            data_type: CatanaDataTypeEnum for the corresponding data type
            (ie. LapData, RunData...)
            dict_runUID: a dict where the keys runUIDs and the values are
            a list of variables the data needs to be retrive from the files.
            years: a list contining the years of the runUIDs. Needs to be
            the same length as the keys of dict_runUID.
            to_json: a boolean indicating if DataFrame returned by
            read_parquet_file needs to be converted to JSON.

        Returns:
            A dict where the keys are runUIDs and the values is a dict with
            keys and values are either JSON or the DataFrame from the read
            files.
        """

        def parallel_read_parquet(files, num_processes):
            """Multiprocess the file reading to make it faster !

            Args:
                files: A list of 3-uplet (runUID, variable, file_path).
                num_processes: The number of processes the function
                read_parquet_file will be run on.

            Returns:
                A list of 3-uplet (runUID, variable, data) where data is
                a DataFrame. 
            """
            if not files:
                return []
            
            nb_files = len(files)
            num_processes = min(num_processes, nb_files)

            if nb_files >= 500:
                logger.warning(f" {len(files)} files to load", stacklevel=6)
            t0 = time.time()
            with Pool(num_processes) as pool:
                dfs = pool.map(self.read_parquet_file, files)
            t1 = time.time()

            logger.info(f" {len(files)} files loaded [{t1-t0:.3f}s]",
                        stacklevel=6)
            return dfs
        
        if len(dict_runUID) != len(years):
            raise HTTPException(
                status_code=400,
                detail='RunUID and Years do not have the same lenght.')


        result = {uid: {} for uid in dict_runUID}
        files = self._get_files_names(data_type, dict_runUID, years)
        num_processes = min(len(files), 6)   # arbitraire
        dfs = parallel_read_parquet(files, num_processes)
        if to_json:
            for uid, var, data in dfs:
                result[uid][var] = data.to_json()
        else:
            for uid, var, data in dfs:
                result[uid][var] = data
        return result

    def list_variables(self) -> dict[str, list[str]]:
        """List all the variables avaliable for this class instance.

        Returns:
            A list (in alphabetical order) of the avaliable data.
        """
        d = {}
        folder = self.folder_datatype[self.data_type]
        for y, uid in zip(self.years, self.run_uid):
            try:
                avaliable_data = [entry.name for entry in os.scandir(
                    f'{self.parquet_path}{y}/{uid}/{folder}')]
            except FileNotFoundError:
                d[uid] = []
                continue

            # retire '.parquet'
            variables = list(map(lambda x: x.split('.')[0], avaliable_data))
            if self.data_type is CatanaDataTypeEnum.HISTO or \
                    self.data_type is CatanaDataTypeEnum.HISTOLAPDATA or \
                    self.data_type is CatanaDataTypeEnum.HISTO2D:
                variables = [
                    var for var in variables if not var.endswith('Axis')]
            d[uid] = sorted(variables)

        return d

    def _encode_run_uid(self, result: pd.DataFrame) -> pd.DataFrame:
        """Encode runUIDs to reduce the size of the data then send to users.

        The convention is : runUID is encoded with its position in the
        list of runUIDs passend when creating the object.
        If a runUID is duplicated, its index will be the last of its
        positions in the list.

        Args:
            result: a DataFrame containing the column RunUID to encode.

        Returns:
            The DataFrame with the column RunUID_index replacing the column
            RunUID.

        Raises:
            KeyError: The DataFrame result do not have a column named
            'RunUID'. 
        """
        if result.empty:
            return result
        
        d = {u: i for i, u in enumerate(
            self.run_uid, 0)}
        array = [d[uid] for uid in result['RunUID']]
        result['RunUID_index'] = array
        result = result.drop(columns='RunUID')

        return result

    @staticmethod
    def _create_interval(data: dict[str, dict[str, list]], uid: str, var: str) -> tuple[list, list]:
        """Given an 'Axis' variable, creates two lists the represent the
        interval.
        Exemple: if the 'Axis' data is [1,2,3,4] ->
        left = [1,2,3] and right = [2,3,4]
        Then the intervals can be interpreted as :
        (left[i], right[i]) = (1,2) ; (2,3) ; (3,4).

        Args:
            data: A dict containing the data retrived from the parquets
            files or the redis cache. Keys are RunUID and values are a
            dict {variable: json_data}.
            uid: The RunUID the interval is created for.
            var: the variable passed to create the interval on.

        Return:
            A 2-uplet of lists (left, right) where the first list contains
            the left edges of the intervals and the second list contains
            the right edges of the intervals.

        Raises:
            ValueError: The variable var passed to create an interval is
            not an Axis variable : it should ends with either 'x_Axis'
            or 'y_Axis'.
        """
        if not var.endswith('Axis'):
            raise ValueError(f'Should be an axis : {var}')

        js = json.loads(data[uid][var])
        var_axis = list(js['Value'].values())
        left = var_axis[:-1]
        right = var_axis[1:]
        return (left, right)

    def _aggregate_df(self, df: pd.DataFrame, by= list[str],
                        agg_ = dict[str, list[str]|str]) -> pd.DataFrame:
        """
            Function to aggregate a dataframe. Aggregated by the by list
            using
            the functions specified in the agg dict.
        """
        t0 = time.time()
        df = df.groupby(by=by).agg(agg_)
        t1 = time.time()
        limit, duration = 0.1, t1-t0
        if  duration > limit:
            logger.warning(f"""Agregation for {self.data_type} took more """
                           f"""than {limit}s [{duration:.3f}s]""")

        df.columns = df.columns.map(lambda x:
                    x[1] if isinstance(x, tuple) and
                        not(str(x[0]).endswith('Left')
                            or str(x[0]).endswith('Right'))
                    else x[0] if isinstance(x, tuple)
                    else x)
        df['RunUID_index'] = 0   # useless when aggregating
        if 'RunUID' in df.columns:
            df = df.drop(columns='RunUID')

        df = df.reset_index()
        return df

    def _check_if_list(self, elem):
        """Check if the parameters passed when creating an instance is a
        list or a single element. 
        
        If it was a single element the object is transformed in a list.
        If it was already a list then it is untouched.

        Args:
            elem: The element to check if it is a list or not.

        Returns:
            elem untouched if it was a list or [elem] if elem was a single
            element.        
        """
        if not isinstance(elem, list):
            return [elem]
        return elem

    def _get_files_names(self, data_type: CatanaDataTypeEnum | str, dict_runUID: dict[str, list[str]],
                         years: list[int]) -> list[tuple[str, str, str]]:
        """Creates a list from the object parameters to create all the file path necessary.
            
            Construct the file path with the prefix and associate the year, 
            RunUID, data_type and variable to create the full file_paths.

        Args:
            data_type: the data_type of the object, needed to get the correct folder.
            dict_runUID: a dict {runUID: list(variables requested)}
            years: a list containing the years of the RunUIDs. Should be in the same order as the RunUIDs.

        Returns:
            A list of 3-uplet (RunUID, variable, file_path).

        Raises:
            HTTPException : caused by a KeyError if the data_type is unknown.
        """
        try:
            folder = self.folder_datatype[data_type]
        except KeyError:
            raise HTTPException(
                status_code=400, detail=f'Unknown data type: {data_type}')

        return [(uid, var, f'{self.parquet_path}{str(year)}/{uid}/{folder}{var}.parquet')
                for uid, year in zip(dict_runUID, years) for var in dict_runUID[uid]
                ]

    def create_dict_runUID(self, variables:list[str] = None) -> dict[str, list[str]]:
        """ Creates the dict to read the parquet files. Assert unicity of runUIDs.

        In case there are duplicated runUIDs, the list self.years is modified to keep
        the same number of elements. 

        Args:
            variables: a list containing the variable to get, if different form self.variables.

        Returns:
            A dict with runUID as key and the associated variables list.
        
        
        """
        dict_runUID = {}
        count_deleted = 0
        for i in range(len(self.run_uid)):
            uid = self.run_uid[i]
            if uid in dict_runUID:
                del self.years[i - count_deleted]   # offset by the number of deleted elements
                count_deleted += 1
            else :
                if variables is not None:
                    dict_runUID[uid] = variables.copy()
                else:
                    dict_runUID[uid] = self.variables.copy()
        return dict_runUID
