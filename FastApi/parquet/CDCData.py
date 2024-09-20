from datetime import datetime
import json
import logging
import time

import numpy as np
import pandas as pd

from bdd.CATANA import CATANA
from parquet.CatanaAggregationEnum import CatanaAggregationEnum
from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.PARQUET import PARQUET

logger = logging.getLogger('main_log')

class CDCData(PARQUET):
    """Implement Parquet class.

    Specialized in treating CDC data.

    Raises:
        NotImplementedError: If an object of this class is created.
        HTTPException: If the competition passed when creating an object is unknown.
    """

    def __init__(self, competition, variables, run_uid, years):
        data_type = CatanaDataTypeEnum.CDCDATA
        super().__init__(competition, variables, run_uid, years, data_type)

    def _process_run(self, run_cdc_data: pd.DataFrame,
                     uid: str, data: dict[str, list],
                     agg_requested: bool) -> dict[str, np.ndarray]:
        """Process data from one Run.

        The data is split by runUID to be processed one by one. 
        This function calls cached_read_files as the cdcUID are function of the runUID.

        Args:
            run_cdc_data: The CDC data retreived either from the parquet file or the cache.
            uid: The UID of the processed run.
            year: The year the run took place.
            update: A boolean indicating if the cache needs to update its values.
            data: the data for a given run retrieved either from parquet or cache.
        """
        t0 = time.time()
        runvar = {'Duration': np.empty(0, dtype=float), 'Occurrences': np.empty(0, dtype=float),
                  'LapCount': np.empty(0, dtype=float), 'Identifier': np.empty(0),
                  'CDCLimitUID': np.empty(0, dtype=np.dtype('U36')),
                  'CDCUID': np.empty(0, dtype=np.dtype('U36'))}
        
        if not agg_requested:
            runvar['RunUID'] = np.empty(0, dtype=np.dtype('U36'))

        for id_, var, limitUID in zip(run_cdc_data['Identifier'],
                                      run_cdc_data['CDCUID'],
                                      run_cdc_data['CDCLimitUID']):
            
            if not var in data:
                continue
            js = json.loads(data[var])
            if not js:
                continue
            lap_count = len(js) - 1
            runvar['Duration'] = np.append(runvar['Duration'], np.array(
                [js[i]['0'] for i in js][1:], dtype=float))

            runvar['Occurrences'] = np.append(runvar['Occurrences'],
                                             np.array([js[i]['1'] for i in js][1:], dtype=float))

            runvar['LapCount'] = np.append(
                runvar['LapCount'], np.arange(1, lap_count + 1, dtype=int))

            runvar['Identifier'] = np.append(
                runvar['Identifier'], np.full(lap_count, id_))

            runvar['CDCUID'] = np.append(runvar['CDCUID'], np.full(
                lap_count, var, dtype=np.dtype('U36')))

            runvar['CDCLimitUID'] = np.append(runvar['CDCLimitUID'], np.full(
                lap_count, limitUID, dtype=np.dtype('U36')))

        if agg_requested:
            runvar['RunUID_index'] = np.full(runvar['CDCUID'].size,
                                             0, dtype=int)
        else:
              runvar['RunUID'] = np.full(runvar['CDCUID'].size,
                                         uid, dtype=np.dtype('U36'))
        
        t1 = time.time()
        limit, duration = 0.5, t1-t0
        if duration > limit:
            logger.warning(f"""Processing {uid} took more than {limit}s """
                            f"""[{duration:.3f}s]""", stacklevel=2)
        return runvar

    def process_data(self, update: bool,
                     agg: CatanaAggregationEnum | list[CatanaAggregationEnum]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process the data for a CDCData instance.

        Gets the cdcUIDs, then read the parquet files needed. 
        There is one call per runUID due to the fact that cdcUID=f(runUID).

        Args:
            update: Boolean to indicate if the cached data needs to be overwritten.

        Returns:
            A pd.DataFrame containing the identifiers, cdcUIDs, data, LapCount per run
            and the runUIDs. 

        Raises:
            HTTPException: If there is a different number of runUIDs and Years.
        """

        cdc_uids = self.get_CDCINFO_from_parquet()
        if not cdc_uids:
            return (cdc_uids,
                    pd.DataFrame(columns=['RunUID_index', 'LapCount']))
        
        dict_runUID = {uid: cdc_uids[uid]['CDCUID'].to_list() 
                       for uid in self.run_uid if 'CDCUID' in cdc_uids[uid]}
        if not dict_runUID:
            return (cdc_uids,
                    pd.DataFrame(columns=['RunUID_index', 'LapCount']))
            
        data = self.cached_read_files(data_type=CatanaDataTypeEnum.CDCDATA,
                                      dict_runUID=dict_runUID, years=self.years,
                                      update=update, competition=self.competition)

        if not data:
            return (cdc_uids, 
                    pd.DataFrame(columns=['RunUID_index', 'LapCount']))
        
        if not isinstance(agg, list):
            agg = [agg]

        agg_requested = (agg != [CatanaAggregationEnum.NONE])
        if (agg != [CatanaAggregationEnum.SUM] and agg_requested):
            if len(agg) > 1:
                logger.error("""Only one aggregation function can be used"""
                           f"""for CDC : {agg}""")
            else:
                logger.warning("""Aggregation other than sum or none used"""
                            f"""for CDC : {agg}""")

        res = [self._process_run(cdc_uids[u], u, data[u], agg_requested)
               for u in dict_runUID.keys() if u in data]
        if not any(res):  # que des None ie. aucun return de runvar
            return (cdc_uids, 
                    pd.DataFrame(columns=['RunUID_index', 'LapCount']))

        res = [pd.DataFrame(i) for i in res]
        res = pd.concat(res).copy()
        
        if agg_requested:
            agg = agg[0].value
            agg_dict = {'Occurrences': agg, 'Duration': agg,
                        'CDCLimitUID':'first'}
            res = self._aggregate_df(res, by=['Identifier', 'CDCUID'],
                                             agg_=agg_dict)
        else:
            res = self._encode_run_uid(res)
        
        return (cdc_uids, res)

    def get_CDCINFO_from_parquet(self) -> dict[str, pd.DataFrame]:
        """Gets the CDC informations for the variables of the instance.
        
        Returns: A dict where keys are runUIDs and values are DataFrame 
        containing CDC informations 
        """
        identifiers = self.variables
        p = CATANA()
        cdc = {}

        dict_runUID = self.create_dict_runUID(variables=['detailed_meta_file'])
        meta_files = self.cached_read_files(
            data_type=CatanaDataTypeEnum.METADATA,
            dict_runUID=dict_runUID, years=self.years, update=False,
            competition=self.competition)
        
        latest_run = {'run_date': 0}
        for u in dict_runUID:
            meta_file = json.loads(meta_files[u]['detailed_meta_file'])
            if not meta_file:
                cdc[u] = pd.DataFrame()
                continue

            run_start = meta_file['StartTime']['0']
            if run_start < 1262304000000: #   01/01/2010 00:00:00.000
                start_time = datetime.fromtimestamp(run_start/1_000)\
                        .isoformat('T', 'milliseconds')
                logger.error(f"""Run {u} has a start time before 2010"""
                             f""" [{start_time}]""")
            if run_start > latest_run['run_date']:
                latest_run['run_date'] = run_start
                latest_run['run_tag'] = meta_file['Type']['0']
                latest_run['engine_type'] = meta_file['EngineType']['0']
                latest_run['competition'] = meta_file['Competition']['0']

        if latest_run['run_date'] == 0:   # no run in the request
            return cdc
        
        latest_run['run_date'] = datetime\
            .fromtimestamp(latest_run['run_date']/1_000)\
            .isoformat('T', 'milliseconds')
        
        cdclistuid = p.get_cdclistuid_from_runmeta(**latest_run)
        if cdclistuid.empty:
            return {}
        cdclistuid = cdclistuid.iloc[0,0]
        cdc_limits = p.get_cdcuid_from_listuid(cdclistuid, identifiers)
        cdc_limits['CDCListUID'] = cdclistuid

        for u in dict_runUID:
            if u in cdc:   # no metadata => no run
                continue
            cdc[u] = cdc_limits
            
        return cdc
