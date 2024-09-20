from datetime import datetime
from sqlalchemy import update
from time import sleep

import uuid
import pandas as pd

from bdd.CATANA import CATANA, RUNFILTER
from utils.filters import filter_eq, filter_like


class CATANAINSERT(CATANA):

    def __init__(self, base="CATANADEV"):
        tables = ['FILELIST', 'SOURCETYPE', 'RUNTAG', 'FILEMANAGER',
                  'TRACKINFO', 'RUNINFOTAGJOIN', 'ENGINETYPE',
                  'RUNMETANAME', 'RUNMETA', 'RUNFILEJOIN']
        super().__init__(base, more_tables=tables)

    def to_test(self, var, var2):
        """
            Not here to stay; only to test a blocking function.
        """
        print(f'Starting SQL with {var} and {var2}')
        sleep(2)   # Blocking
        print(f'Done : {var} and {var2}')
        print(f"{'-'*40}")

    def insert_new_run(self, run_meta: dict[str, any]) -> str:
        """
            Ordre :
            1) récuper sourcejoin_id  (DONE)
            2) vérifier trackinfo et enginetype => ajouter si new ou récup id (DONE IN RUNINFO)
            3) insert RUNINFO (DONE)
            4) insert RUNINFOTAGJOIN (runinfo_id de l'insert en 3 et récup RUNTAG_id) (DONE)
            5) insert RUNMETA (runinfo_id de l'insert 3)
            6) insert FILELIST (sourcejoin_id récup en 1)
            7) insert RUNFILEJOIN (runinfo_id de 3 et filelist_id de 6)
            8) insert FILEMANAGER (filelist_id de 6)

            Suppression : RUNINFO suffit

            run_meta : dict containing all the run infos avaliable.

            Returns : RunUID of the run
        """

        # Check if the run is already in the db.
        run_already_in = self.get_runs_already_in_db(run_meta['RunFileName'])
        run_meta['RunCompleted'] = run_meta.pop('Complete')   # rename key

        if run_already_in:
            self.update_RUNINFO(run_already_in, run_meta)
            print(f'Update RUNINFO for RunUID : {run_already_in}')
            return run_already_in
        

        run_id, run_uid = self.insert_RUNINFO(run_meta)
        self.insert_RUNINFOTAGJOIN(run_id, run_meta)
        self.insert_RUNMETA(run_id, run_meta)
        file_list_id = self.insert_FILELIST(run_meta)
        self.insert_RUNFILEJOIN(run_id, file_list_id)
        self.insert_FILEMANAGER(file_list_id, priority=1, to_process=1)

        return str(run_uid).upper()

    def insert_FILELIST(self,
                      run_meta: dict[str, any]) -> int:
        """
            Insert into FILELIST. 
            Needs : [Source, Folder, FileName, LastModified, Telemetry,
                    TimeOfRecording, SessionName, SessionDescription]

            Returns : The FILELIST id in the DB.
        """
        t = self.tables
        columns_to_insert = t.FILELIST.c.keys().copy()
        columns_to_insert.remove('id')


        source_types = self.get_SOURCETYPE()
        src_type = run_meta['Source']
        src_type = source_types[src_type]

        competitions = self.get_COMPETITION()
        competition_id = competitions[run_meta['Competition']]

        run_meta['SOURCEJOIN_id'] = self.get_SOURCEJOIN_id(competition_id, src_type)
        run_meta['Folder'] = run_meta.pop('FilePath')

        to_insert = {k: run_meta[k] for k in columns_to_insert 
                     if k in run_meta}

        insert = t.FILELIST\
                        .insert()\
                        .values(to_insert)\
                        .returning(
                            t.FILELIST.c.id
                        )
        file_list_id = self._execute_sql(insert, fetch=True)
        return file_list_id[0][0]

    def insert_RUNINFO(self,
                       run_info: dict[str, any]) -> tuple[int, uuid.UUID]:
        """"
        Insert into RUNINFO. RunUID can be specified in the run_infos.

        Needs : ['RunFileName', 'Competition', 'Track', 'EngineType', 'StartTime',
            'EndTime', 'NbLaps', 'Team', 'Driver', 'Event', 'Session', 'RunType',
            'RunNumber', 'RunCompleted', 'RunName', 'RunDescription', 'RunTag']

        tested input : df.to_json(orient='records', index=False, date_format="iso")
        Returns a tuple(id, UUID) with id needed for the RUNMETA 
        and RUNINFOTAGJOIN inserts.
        """
        t = self.tables
        columns_to_insert = t.RUNINFO.c.keys().copy()
        columns_to_insert.remove('id')

        source_types = self.get_SOURCETYPE()
        competitions = self.get_COMPETITION()
        src_type = source_types[run_info['Source']]
        competition_id = competitions[run_info['Competition']]

        run_info['SOURCEJOIN_id'] = self.get_SOURCEJOIN_id(
            competition_id, src_type)
        
        run_info['TRACKINFO_id'] = self.get_TRACKINFO_id(
            competition_id, run_info['Track'])
        
        run_info['ENGINETYPE_id'] = self.get_ENGINETYPE_id(
            competition_id, run_info['EngineType'])
        

        run_info = self.format_datetime(run_info, 
                    ['DateofRecording', 'StartTime', 'EndTime', 'LastModified'])

        # filter to keep only needed columns
        if not 'RunUID' in run_info:
            columns_to_insert.remove('RunUID')

        to_insert = {k: run_info[k] for k in columns_to_insert 
                     if k in run_info}

        insert = t.RUNINFO\
                  .insert()\
                  .values(to_insert)\
                  .returning(
                      t.RUNINFO.c.id,
                      t.RUNINFO.c.RunUID
                  )

        run_uid = self._execute_sql(insert, fetch=True)

        st = '*'*40
        print(f"{st}\nNew lap inserted : {str(run_uid[0][0]).upper()}\n{st}")
        return (run_uid[0][0], run_uid[0][1]) 

    def get_SOURCETYPE(self) -> dict[str, int]:
        """
            Returns a dict containing all the source types as key and their id as value.
        """
        t = self.tables
        select = []
        self._select(select, t.SOURCETYPE, keep_id=True)
        query = self._create_query(select)
        df = self._read_sql(query)
        return dict(zip(df['SourceType'], df['id']))

    def get_LAPTYPE_id(self, lap_type: str) -> int:
        """
            Returns a dict containing all the lap types as key and their id as value.
        """
        t = self.tables
        select = []
        self._select(select, t.LAPTYPE, keep_id=True)
        query = self._create_query(select)

        query = filter_eq(query, t.LAPTYPE.c.Type, lap_type)
        df = self._read_sql(query)
        if df.empty:   # new LapType
            insert = t.LAPTYPE\
                      .insert()\
                      .values(
                          {'Type': lap_type, 'NominalLap': 1}
                      )\
                      .returning(t.LAPTYPE.c.id)
            laptype_id = self._execute_sql(insert, fetch=True)
            return laptype_id[0][0]

        return int(df['id'].iloc[0])

    def get_ENGINETYPE_id(self,
                          competition_id: int,
                          engine: str) -> int:
        """
            Get id from ENGINETYPE. ENGINETYPE is defined by a Competition and an Engine.
            Using their ids.
        """
        t = self.tables
        select = []
        self._select(select, t.ENGINETYPE, keep_id=True)
        query = self._create_query(select)

        query = filter_eq(query, t.ENGINETYPE.c.COMPETITION_id, competition_id)
        query = filter_eq(query, t.ENGINETYPE.c.EngineType, engine)

        df = self._read_sql(query)

        if df.empty:   # New EngineType
            insert = t.ENGINETYPE\
                      .insert()\
                      .values(
                          {'EngineType': engine,'COMPETITION_id': competition_id}
                      )\
                      .returning(t.ENGINETYPE.c.id)
            
            engine_id = self._execute_sql(insert, fetch=True)
            return engine_id[0][0]

        if df.shape[0] > 1:
            raise ValueError(
                f'Multpile ids for COMPETITION_id = {competition_id} and Engine = {engine}')

        return int(df['id'].iloc[0])

    def get_TRACKINFO_id(self,
                         competition_id: int,
                         track_name: str) -> int :
        """
            Get TrackInfo_id or insert a new track in the db in case it's a new track.
            Needed : Competition (id) and TrackName.

            Returns the id.
        """
        t = self.tables
        select = []
        self._select(select, t.TRACKINFO, keep_id=True)
        query = self._create_query(select)

        query = filter_eq(query, t.TRACKINFO.c.COMPETITION_id, competition_id)
        query = filter_eq(query, t.TRACKINFO.c.Track, track_name)

        df = self._read_sql(query)

        if df.empty:   # New Track
            insert = t.TRACKINFO\
                      .insert()\
                      .values(
                          {'Track': track_name, 'COMPETITION_id': competition_id}
                      )\
                      .returning(t.TRACKINFO.c.id)
            track_id = self._execute_sql(insert, fetch=True)
            return track_id[0][0]

        if df.shape[0] > 1:
            raise ValueError(
                f'Multpile ids for COMPETITION_id = {competition_id} and Track = {track_name}')

        return int(df['id'].iloc[0])

    def insert_RUNINFOTAGJOIN(self,
                              run_id : int,
                              run_info: dict[str, any]) -> None:
        """
            Insert into RUNINFOTAGJOIN.

            run_info = {runinfo (including RunFileName) & avaliable runmeta}
        """
        t = self.tables
        def get_RUNTAG_id() -> dict[str, int]:
            """
                Returns a dict {RunTag: id} (ie. all the RUNTAG table).
            """
            select = []
            self._select(select, t.RUNTAG, keep_id=True)
            query = self._create_query(select)
            df = self._read_sql(query)
            return dict(zip(df['RunTag'], df['id']))
        
        runtag_ids = get_RUNTAG_id()
        runtag = run_info['Type']

        to_insert = {'RUNINFO_id' : run_id,
                     'RUNTAG_id' : runtag_ids[runtag]}

        insert = t.RUNINFOTAGJOIN.insert().values(to_insert)
        self._execute_sql(insert)

    def insert_RUNMETA(self,
                       run_id: int,
                       run_infos: dict[str, any]) -> None:
        """
            Insert into RUNMETA.

            run_infos = {runinfo (including RunFileName) & avaliable runmeta}
        """
        t = self.tables

        def get_run_meta_names() -> dict[str, int]:
            """
                Returns a dict {RunMetaName: id} (ie. the table RUNMETANAME).
            """
            select = list()
            self._select(select, t.RUNMETANAME, keep_id=True)
            query = self._create_query(select)
            df = self._read_sql(query)
            return dict(zip(df['Name'], df['id']))
        
        run_meta_names = get_run_meta_names()

        ready_to_insert = []
        for rm_name in run_meta_names:
            to_insert = {}
            to_insert['RUNINFO_id'] = run_id
            to_insert['RUNMETANAME_id'] = run_meta_names[rm_name]
            if rm_name in run_infos:
                to_insert['Value'] = run_infos[rm_name]
                to_insert['OriginalValue'] = run_infos[rm_name]
            else:
                to_insert['Value'] = None
                to_insert['OriginalValue'] = None

            ready_to_insert.append(to_insert)

        multi_insert = t.RUNMETA.insert().values(ready_to_insert)
        self._execute_sql(multi_insert)

    def insert_RUNFILEJOIN(self,
                           run_id: int,
                           file_id: int) -> None:
        """
            Insert into RUNFILEJOIN.
        """

        to_insert = {
            'RUNINFO_id': run_id,
            'FILELIST_id' : file_id
        }

        t = self.tables
        insert = t.RUNFILEJOIN.insert().values(to_insert)
        self._execute_sql(insert)

    def insert_FILEMANAGER(self,
                           file_id: int,
                           priority: int,
                           to_process: int) -> None:
        """
            Insert into FILEMANAGER.
        """
        t = self.tables
        to_insert = {
            'FILELIST_id': file_id,
            'Priority': priority,
            'ToProcess' : to_process
        }

        insert = t.FILEMANAGER.insert().values(to_insert)
        self._execute_sql(insert)

    def get_runs_already_in_db(self, run_info_name: str) -> str:
        """
            check if a run with a given name is already in the DB 
            (ie. inserted in RUNFILEMETA).

            Returns the RunUID if the run is already in the DB or an
            empty string.
        """

        run_filter = RUNFILTER()
        run_filter.from_dict({
            'FileName': run_info_name
        })
        get_runs = self.run_get(run_filter)
        if get_runs.empty:
            return ''

        return get_runs['RunUID'].iloc[0]

    def insert_LAPINFO(self,
                     lap_info: dict[str, any]) -> uuid.UUID:
        """
        Insert into LAPINFO.
        Needs : ['RunUID', 'LapType', 'LapNumber', 'StartTime', 
        'EndTime', 'Duration', 'Description', 'FastLap]

        Mandatory : LapType, StartTime, EndTime and RunUID as the key. 

        lap_info = a dict containing all the lap_info avaliable.

        Returns the LapUID.
        """
        t = self.tables

        run_id = self.get_runinfoID_from_runuid([lap_info['RunUID']])
        laptype_id = self.get_LAPTYPE_id(lap_info['LapType'])

        lap_info['RUNINFO_id'] = run_id
        lap_info['LAPTYPE_id'] = laptype_id
        lap_info['FastLap'] = lap_info.pop('FastestLap')
        lap_info = self.format_datetime(lap_info, ['StartTime', 'EndTime'])

        del lap_info['LapType']
        del lap_info['RunUID']

        self.update_RUNINFO_lapNumber(lap_info['LapNumber'], run_id)
        insert = t.LAPINFO.insert().values(lap_info).returning(
             t.LAPINFO.c.LapUID
        )

        lap_uid = self._execute_sql(insert, fetch=True)

        st = '*'*40
        print(f"{st}\nNew lap inserted : {str(lap_uid[0][0]).upper()}\n{st}")

        return str(lap_uid[0][0]).upper()  

    def update_RUNINFO_lapNumber(self, lap_number: int, run_id: int) -> None:
        """
            Update NbLaps in RUNINFO only if the LapNumber is greater 
            than the one in the db for the given run.
        """
        t = self.tables

        to_update = update(t.RUNINFO).values({'NbLaps': lap_number})
        to_update = to_update.where(t.RUNINFO.c.id == run_id)
        to_update = to_update.where(t.RUNINFO.c.NbLaps < lap_number)

        self._execute_sql(to_update)

    def update_RUNINFO(self,
                        run_uid: uuid.UUID,
                        run_info: dict[str, any]) -> None:
        """
            Update RUNINFO data in the SQL DB if the Run was already
            inserted.
            In case some info change (eg. the RunType can change).

            Args:
                run_uid (UUID/str): the RunUID corresponding of the run
                in the database.
                run_info (dict): the dict containing all the run infos
                avaliable

            Returns:
                None, updates the row in the database.
        """
        
        t = self.tables
        columns_to_insert = t.RUNINFO.c.keys().copy()
        columns_to_insert.remove('id')

        run_info = self.format_datetime(run_info, 
                    ['DateofRecording', 'StartTime', 'EndTime', 'LastModified'])

        to_insert = {k: run_info[k] for k in columns_to_insert 
                    if k in run_info}          
        if 'RunUID' not in run_info:
            to_insert['RunUID'] = run_uid

        to_update = update(t.RUNINFO).values(to_insert)
        to_update = to_update.where(t.RUNINFO.c.RunUID == run_uid)
        self._execute_sql(to_update)

    def get_runinfoID_from_runuid(self,
                                  run_uids: str) -> int:
        """
            Get from the DB the correspondance between RunUID and its id in the db.

            Returns the id of the requested RunUID
        """
        t = self.tables
        select = list()
        self._select(select, t.RUNINFO, ['id', 'RunUID'], keep_id=True)
        query = self._create_query(select)

        query = filter_like(query, t.RUNINFO.c.RunUID, run_uids)
        run_ids = self._read_sql(query)

        return int(run_ids['id'].iloc[0])

    @staticmethod
    def format_datetime(dict_: dict, to_check: list[str]):
        for key in to_check:
            if dict_[key] != dict_[key]:   #ie. nan
                dict_[key] = None
            elif dict_[key] and not isinstance(dict_[key], str):
                dict_[key] = datetime.fromtimestamp(dict_[key])\
                                        .isoformat('T', 'milliseconds')
        return dict_