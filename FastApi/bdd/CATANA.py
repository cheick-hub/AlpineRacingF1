from collections import defaultdict
import logging
import uuid

import pandas as pd
import yaml
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from sqlalchemy import select as sqlalchemy_select
import sqlparse   # used for printing queries for debug

from bdd.base import BASEBDD
from utils.filters import filter_eq, filter_like

logger = logging.getLogger('main_log')

class CATANA(BASEBDD):

    def __init__(self, base="CATANA", more_tables=None):
        file = './config/catana.yml'
        with open(file, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        self._create_engine(config, base)

        # Table construction
        tables = ['ALIASVIEW', 'CDCJOINED', 'CDCVIEW', 'COMPETITION',
                  'FILEMANAGER', 'FILEMANAGERVIEW', 'LAPINFO', 'LAPTYPE',
                  'RUNFILEVIEW', 'RUNINFO', 'RUNINFOTAGJOIN', 'RUNINFOVIEW',
                  'RUNMETAMANDATORY', 'RUNMETAVIEW', 'RUNTAG',
                  'RUNTAGCOMPETITIONJOIN', 'SOURCEJOIN', 'TOPROCESSVIEW',
                  'VARINFO']
        
        if more_tables:
            tables.extend(more_tables)

        self._init_table(tables)
        print(base + " DATABASE LOADED")

    def get_Varinfo_name(self):
        t = self.tables
        select = list()
        self._select(select, t.VARINFO, ['Name'])
        query = self._create_query(select)
        df = self._read_sql(query)
        return df

    def alias_get(self, BeginDate, EndDate, Competition, Type):
        t = self.tables
        select = list()
        self._select(select, t.ALIASVIEW)

        query = self._create_query(select)

        query = query.filter(t.ALIASVIEW.c.Competition == Competition)
        query = query.filter(t.ALIASVIEW.c.RunTag == Type)
        query = query.filter(
            or_(t.ALIASVIEW.c.BeginDate <= BeginDate, t.ALIASVIEW.c.BeginDate.is_(None)))
        query = query.filter(
            or_(t.ALIASVIEW.c.EndDate >= EndDate, t.ALIASVIEW.c.EndDate.is_(None)))
        query = query.order_by(t.ALIASVIEW.c.Competition, t.ALIASVIEW.c.RunTag, t.ALIASVIEW.c.Alias,
                               t.ALIASVIEW.c.Priority)
        df = self._read_sql(query)

        return df

    def laptype_get(self):
        t = self.tables
        select = list()
        self._select(select, t.LAPTYPE)

        to_remove = ["id"]
        new_select = []
        for i in range(len(select)):
            if select[i].name not in to_remove:
                new_select.append(select[i])

        query = self._create_query(new_select)
        df = self._read_sql(query)
        return df

    def run_get(self, run_filter=None, component_meta=False):
        t = self.tables
        select = list()
        self._select(select, t.RUNFILEVIEW, keep_id=True)

        # to_remove = ["RunTag"]
        # new_select = [x for x in select if x.name not in to_remove]

        query = self._create_query(select)

        query = self._filter_run_from_meta(query, run_filter)
        # query = query.distinct()
        df = self._read_sql(query)
        df.RunUID = df.RunUID.astype('str')
        df.RunUID = df.RunUID.str.upper()

        if component_meta and not df.empty:
            source = df['Source'].iloc[0]  # all runs have the same
            meta = self._get_RUNMETAVIEW_from_RUNUID(query, source)
            df = df.merge(meta, on='RUNINFO_id', suffixes=(None, '_remove'))
            df = df.drop(columns=df.filter(regex='_remove').columns)

        df = df.drop(columns='RUNINFO_id')
        return df

    def _get_RUNMETAVIEW_from_RUNUID(self,
                                     q_runuid : list[str],
                                     source: str) -> pd.DataFrame:
        """
            Function used to get the metadata form a list of runUIDs.
        """
        t = self.tables
        select = list()
        self._select(select, t.RUNMETAVIEW, ['Name', 'Value', 'RUNINFO_id'])
        # self._select(select, t.RUNFILEVIEW, ['RUNINFO_id'])
        query = self._create_query(select)

        meta_to_keep = self.list_element(source)
        meta_to_keep = meta_to_keep['Element'].tolist()

        subq_runuid = q_runuid.subquery('sq')
        query = query.filter(t.RUNMETAVIEW.c.RUNINFO_id == subq_runuid.c.RUNINFO_id)
        # query = filter_eq(query, t.RUNFILEVIEW.c.RUNINFO_id,
        #                   t.RUNMETAVIEW.c.RUNINFO_id)
        # query = query.join(t.RUNFILEVIEW,
        #             t.RUNFILEVIEW.c.RUNINFO_id == t.RUNMETAVIEW.c.RUNINFO_id)
        query = filter_eq(query, t.RUNMETAVIEW.c.Name, meta_to_keep)

        meta = self._read_sql(query)
        meta = meta.pivot(index='RUNINFO_id', columns='Name')
        meta.columns = meta.columns.to_flat_index()
        meta.columns = meta.columns.map(lambda x: x[1]
        if isinstance(x, tuple)
        else x)
        meta = meta.reset_index()
        # meta['RunUID'] = meta['RunUID'].astype('str').str.upper()

        return meta

    def get_runmeta_names(self, source: str) -> list[str]:
        t = self.tables
        select = []
        self._select(select, t.RUNMETAMANDATORY, ['Name'])
        query = self._create_query(select)
        query = filter_eq(query, t.SOURCEJOIN.c.Name, source)
        query = query.join(t.SOURCEJOIN,
                        t.SOURCEJOIN.c.id == t.RUNMETAMANDATORY.c.SOURCEJOIN_id)
        # query = filter_eq(query, t.RUNMETAMANDATORY.c.SOURCEJOIN_id,
        #                   t.SOURCEJOIN.c.id)

        df = self._read_sql(query)
        return df['Name'].tolist()

    def list_element(self, source=None) -> pd.DataFrame:
        t = self.tables
        select = t.RUNFILEVIEW.c.keys().copy()
        to_remove = ['RUNINFO_id', 'RunUID', 'StartTime', 'EndTime', 'NbLaps',
                     'RunNumber', 'Source', 'RunFileName', 'RunCompleted',
                     'RunDescription', 'RunType']

        meta_names = self.get_runmeta_names(source)
        select.extend(meta_names)
        select.append('RunTag')

        all_elem = [x for x in select if x not in to_remove]
        all_elem.append("RunTag")
        res = pd.DataFrame(all_elem, columns=['Element']) \
            .drop_duplicates(ignore_index=True)
        return res

    def list_value(self, source: str = None, element: str = None,
                   filter_element: str|list[str] = None,
                   filter_value: str|list[str] = None,
                   keep_only_run_to_process: bool = False):
        
        if type(filter_element) != type(filter_value):
            raise TypeError("FilterElements and FilterValues should have "
                            "the same type : both str of both list")
        
        if (isinstance(filter_element, list) 
            and len(filter_element) != len(filter_value)):
            raise ValueError("FilterElements and FilterValues should have"
                             " the same length")

        t = self.tables
        runfileview_columns = t.RUNFILEVIEW.c.keys().copy()
        if filter_element is not None:
            select = []
            if element == 'RunTag':
                self._select(select, t.RUNTAG, element)
                query = self._create_query(select)
                query = query.select_from(t.RUNFILEVIEW)
            elif element in runfileview_columns:
                self._select(select, t.RUNFILEVIEW, element)
                query = self._create_query(select)
            else:   # column in runmetaview
                self._select(select, t.RUNMETAVIEW, 'Value')
                query = self._create_query(select)
                query = query.select_from(t.RUNFILEVIEW)

            dict_filters = defaultdict(list)
            for i, filter_el in enumerate(filter_element):
                dict_filters[filter_el].append(filter_value[i])

            for filter_el in dict_filters:
                filter_val = dict_filters[filter_el]
                if filter_el == 'RunTag':
                    query = query.join(t.RUNINFOTAGJOIN,
                                       t.RUNINFOTAGJOIN.c.RUNINFO_id 
                                       == t.RUNFILEVIEW.c.RUNINFO_id)
                    query = query.join(t.RUNTAG,
                                       t.RUNINFOTAGJOIN.c.RUNTAG_id
                                       == t.RUNTAG.c.id)
                    
                    query = filter_eq(query, t.RUNTAG.c.RunTag, filter_val)
                elif filter_el in runfileview_columns:
                    query = filter_eq(query, t.RUNFILEVIEW.c[filter_el],
                                      filter_val)
                else:   # filter on a meta value
                    alias_rmv = aliased(t.RUNMETAVIEW)
                    query = filter_eq(query, alias_rmv.c.Name, filter_el)
                    query = filter_eq(query, alias_rmv.c.Value, filter_val)
                    query = query.join(alias_rmv,
                        t.RUNFILEVIEW.c.RUNINFO_id == alias_rmv.c.RUNINFO_id)   
            
            if element == 'RunTag':
                if element not in dict_filters:
                    # otherwise, same join twice => error
                    query = query.join(t.RUNINFOTAGJOIN,
                                    t.RUNFILEVIEW.c.RUNINFO_id 
                                    ==  t.RUNINFOTAGJOIN.c.RUNINFO_id)
                    query = query.join(t.RUNTAG,
                                        t.RUNTAG.c.id
                                        == t.RUNINFOTAGJOIN.c.RUNTAG_id)
                
            elif element not in runfileview_columns:
                query = filter_eq(query, t.RUNMETAVIEW.c.Name, element)
                query = query.join(t.RUNMETAVIEW,
                    t.RUNFILEVIEW.c.RUNINFO_id ==  t.RUNMETAVIEW.c.RUNINFO_id)

        if keep_only_run_to_process:
            sq = sqlalchemy_select(t.TOPROCESSVIEW.c.RUNINFO_id)
            query = query.filter(t.RUNFILEVIEW.c.RUNINFO_id.in_(sq))

        query = filter_eq(query, t.RUNFILEVIEW.c.Source, source)
        query = query.distinct()
        logger.debug('\n'+sqlparse.format(str(query), reindent=True))
        df = self._read_sql(query)
        if 'Value' in df.columns:
            df = df.rename(columns={'Value': element})
        return df

    def _filter_run_from_meta(self, q, run_filter=None):
        t = self.tables

        q = filter_eq(q, t.RUNFILEVIEW.c.EngineType, run_filter.EngineType)
        q = filter_eq(q, t.RUNFILEVIEW.c.Driver, run_filter.Driver)
        q = filter_eq(q, t.RUNFILEVIEW.c.Event, run_filter.Event)
        q = filter_eq(q, t.RUNFILEVIEW.c.Session, run_filter.Session)
        q = filter_eq(q, t.RUNFILEVIEW.c.RunType, run_filter.RunType)
        q = filter_eq(q, t.RUNFILEVIEW.c.RunNumber, run_filter.RunNumber)
        q = filter_eq(q, t.RUNFILEVIEW.c.RunFileName, run_filter.FileName)
        q = filter_eq(q, t.RUNFILEVIEW.c.RunUID, run_filter.RunUID)
        q = filter_eq(q, t.RUNFILEVIEW.c.Track, run_filter.Track)
        q = filter_like(q, t.RUNFILEVIEW.c.RunDescription,
                        run_filter.SessionDescription)
        q = filter_like(q, t.RUNFILEVIEW.c.Source, run_filter.Competition)
        q = filter_like(q, t.RUNFILEVIEW.c.RunName, run_filter.SessionName)
        if run_filter.RunTag:
            q = q.join(t.RUNINFOTAGJOIN,
                       t.RUNFILEVIEW.c.RUNINFO_id == t.RUNINFOTAGJOIN.c.RUNINFO_id)
            q = q.join(t.RUNTAG, t.RUNTAG.c.id == t.RUNINFOTAGJOIN.c.RUNTAG_id)
            q = filter_eq(q, t.RUNTAG.c.RunTag, run_filter.RunTag)

        if run_filter.Component is not None:
            # q = q.join(t.RUNMETAVIEW, 
            #            t.RUNMETAVIEW.c.RUNINFO_id == t.RUNFILEVIEW.c.RUNINFO_id)
            # q = filter_eq(q,
            #               t.RUNMETAVIEW.c.RUNINFO_id,
            #               t.RUNFILEVIEW.c.RUNINFO_id)
            for i, (name, value) in enumerate(run_filter.Component.items()):
                sub_select = []
                self._select(sub_select, t.RUNMETAVIEW, ['RUNINFO_id'])
                subquery = self._create_query(sub_select)
                subquery = filter_eq(subquery, t.RUNMETAVIEW.c.Name, name)
                subquery = filter_eq(subquery, t.RUNMETAVIEW.c.Value, value)
                # subquery = subquery.filter(t.RUNFILEVIEW.c.RUNINFO_id == subquery.c.RUNINFO_id)
                subquery = subquery.subquery(f'subquery{i}')

                q = q.filter(t.RUNFILEVIEW.c.RUNINFO_id == subquery.c.RUNINFO_id)
                # q = filter_eq(q, t.RUNFILEVIEW.c.RUNINFO_id,
                #               subquery.c.RUNINFO_id)

        return q

    def cdc_list(self, Competition, EngineType, RunTag):
        t = self.tables
        select = list()
        self._select(select, t.CDCVIEW)
        query = self._create_query(select)
        query = filter_like(query, t.CDCVIEW.c.Competition, Competition)
        query = filter_like(query, t.CDCVIEW.c.EngineType, EngineType)
        query = filter_like(query, t.CDCVIEW.c.RunTag, RunTag)
        query = query.order_by(t.CDCVIEW.c.Version.desc())
        df = self._read_sql(query)

        df.CDCListUID = df.CDCListUID.astype('str')
        df.CDCListUID = df.CDCListUID.str.upper()

        return df

    def cdc_get(self, CDCListUID, CDCUID, CDCLimitUID, Identifier):
        t = self.tables
        select = list()
        self._select(select, t.CDCJOINED)
        query = self._create_query(select)

        query = filter_like(query, t.CDCJOINED.c.CDCListUID, CDCListUID)
        query = filter_like(query, t.CDCJOINED.c.CDCUID, CDCUID)
        query = filter_like(query, t.CDCJOINED.c.CDCLimitUID, CDCLimitUID)
        query = filter_like(query, t.CDCJOINED.c.Identifier, Identifier)

        df = self._read_sql(query)

        df.CDCListUID = df.CDCListUID.astype('str')
        df.CDCListUID = df.CDCListUID.str.upper()
        df.CDCUID = df.CDCUID.astype('str')
        df.CDCUID = df.CDCUID.str.upper()
        df.CDCLimitUID = df.CDCLimitUID.astype('str')
        df.CDCLimitUID = df.CDCLimitUID.str.upper()
        return df

    def lap_meta_get(self, run_uid: list[str] | str) -> pd.DataFrame:
        if len(run_uid) == 0:
            return pd.DataFrame()

        t = self.tables
        select = list()
        self._select(select, t.RUNINFOVIEW, ['RunUID'])
        self._select(select, t.LAPINFO, [
            'LapNumber', 'StartTime', 'Description'])
        self._select(select, t.LAPTYPE, ['NominalLap', 'Type'])

        query = self._create_query(select)
        query = query.join(
            t.RUNINFOVIEW, t.RUNINFOVIEW.c.RUNINFO_id == t.LAPINFO.c.RUNINFO_id)
        query = query.join(t.LAPTYPE, t.LAPINFO.c.LAPTYPE_id == t.LAPTYPE.c.id)
        query = query.order_by(t.LAPINFO.c.StartTime.asc())
        query = query.distinct()
        valid_run_uid = self._keep_valid_uuid(run_uid)
        query = filter_eq(query, t.RUNINFOVIEW.c.RunUID, valid_run_uid)

        if len(valid_run_uid) == 0:
            return pd.DataFrame(columns=['Valid_RunUID'])

        df = self._read_sql(query)
        df.RunUID = df.RunUID.astype('str')
        df.RunUID = df.RunUID.str.upper()
        df['LapCount'] = df.groupby('RunUID').cumcount() + 1
        return df

    def get_cdclistuid_from_runmeta(self, engine_type: str,
                                    run_tag: str, competition: str,
                                    run_date: str):
        if isinstance(run_date, list):
            return pd.DataFrame(columns=['RunDate'])

        t = self.tables
        select_sub = list()
        self._select(select_sub, t.CDCVIEW, ['CDCListUID'])
        query = self._create_query(select_sub)
        query = filter_eq(query, t.CDCVIEW.c.EngineType, engine_type)
        query = filter_eq(query, t.CDCVIEW.c.RunTag, run_tag)
        query = filter_eq(query, t.CDCVIEW.c.Competition, competition)
        query = query.filter(
            or_(t.CDCVIEW.c.EndDate > run_date, t.CDCVIEW.c.EndDate == None))
        query = query.filter(t.CDCVIEW.c.StartDate < run_date)
        query = query.order_by(t.CDCVIEW.c.Version.desc())

        df = self._read_sql(query)
        if df.empty:
            return df

        df.CDCListUID = df.CDCListUID.astype('str').str.upper()
        return df

    def get_cdcuid_from_listuid(self, cdc_list_uid: str,
                                identifiers: list[str]) -> pd.DataFrame:
        t = self.tables
        select = []
        self._select(select, t.CDCJOINED,
                     ['CDCType', 'Identifier', 'CDCUID', 'CDCLimitUID',
                      'Category', 'Description', 'Comment', 'Channel', 'Unit',
                      'Criterion', 'Value', 'Duration', 'Occurrences',
                      'Conditions'])

        query = self._create_query(select)
        query = filter_eq(query, t.CDCJOINED.c.Identifier, identifiers)
        query = filter_eq(query, t.CDCJOINED.c.CDCListUID, cdc_list_uid)
        df = self._read_sql(query)
        if df.empty:
            return df

        df.CDCUID = df.CDCUID.astype('str').str.upper()
        df.CDCLimitUID = df.CDCLimitUID.astype('str').str.upper()
        return df

    def _keep_valid_uuid(self, val: list[str] | str) -> list[str]:
        if not isinstance(val, list):
            val = [val]

        valid_uids = []
        for v in val:
            try:
                uuid.UUID(str(v))
                valid_uids.append(v)
            except ValueError:
                continue

        return valid_uids

    def get_new_processed_RUNUID(self, competition: str, limitDate: str):
        t = self.tables
        select = list()
        self._select(select, t.FILEMANAGERVIEW, ['RunUID', 'TimeOfRecording'])
        query = self._create_query(select)
        query = filter_eq(
            query, t.FILEMANAGERVIEW.c.SOURCEJOIN_Name, competition)
        query = query.filter(
            or_(t.FILEMANAGERVIEW.c.ProcessingEnd3 > limitDate, t.FILEMANAGERVIEW.c.ProcessingEndEUL > limitDate))
        query = query.distinct()
        df = self._read_sql(query)
        return df

    def get_COMPETITION(self) -> dict[str, int]:
        """
            Returns a dict containing all the competition as key and
            their id as value.
        """
        t = self.tables
        select = []
        self._select(select, t.COMPETITION, keep_id=True)
        query = self._create_query(select)
        df = self._read_sql(query)
        return dict(zip(df['Competition'], df['id']))

    def get_SOURCEJOIN_id(self,
                          competition_id: int,
                          source_type_id: int) -> int:
        """
            Get id from SOURCEJOIN. SOURCEJOIN is defined by a Competition
            and a SourceType.
            Using their ids.
        """
        t = self.tables
        select = []
        self._select(select, t.SOURCEJOIN, keep_id=True)
        query = self._create_query(select)

        query = filter_eq(query, t.SOURCEJOIN.c.COMPETITION_id, competition_id)
        query = filter_eq(query, t.SOURCEJOIN.c.SOURCETYPE_id, source_type_id)

        df = self._read_sql(query)

        if df.empty:
            raise ValueError(
                f'Unknown SourceType for COMPETITION_id = {competition_id} and SOURCETYPE_id = {source_type_id}'
            )

        if df.shape[0] > 1:
            raise ValueError(
                f'Multpile ids for COMPETITION_id = {competition_id} and SOURCETYPE_id = {source_type_id}')

        return int(df['id'].iloc[0])


class RUNFILTER:
    """
    class that permits to filtre runs
    """
    keys = ["Car", "Chassis", "ChassisNumber", "EngineType", "PU", "Component",
            "Track", "Driver", "Event", "Session", "RunType", "RunNumber",
            "FileName", "SessionName", "SessionDescription", "Type", "RunUID",
            "Competition", "RunTag"]

    def __init__(self):

        for key in self.keys:
            setattr(self, key, None)

    def from_dict(self, d):
        """
        set filter attribute from dict
        :param d:
        """
        for keys in d:
            setattr(self, keys, d[keys])

    def __setattr__(self, key, value):
        if key not in self.keys:
            return
        if (not isinstance(value, type([])) and value is not None
                and not isinstance(value, dict)):
            value = [value]
        if value is not None and len(value) == 0:
            value = None
        super(RUNFILTER, self).__setattr__(key, value)

    def is_none(self):
        """
        Check if any field is set
        :return: boolean
        """
        variables = self.__dict__
        for key in variables:
            if variables[key] is not None:
                return False
        return True
