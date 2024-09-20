import json

import numpy as np
import yaml
from fastapi import Header, HTTPException
from sqlalchemy import false
from sqlalchemy.sql import func

from bdd.base import BASEBDD
from utils.filters import filter_eq, filter_like


class PUAS3(BASEBDD):
    def __init__(self, base="PUAS3"):

        file = './config/puas.yml'
        with open(file, 'r') as file:
            config = yaml.safe_load(file)
        self._create_engine(config, base)

        tables = ["FILEINFO", "FILELIST", "RUNINFO", "META", "COMPONENT", "VARINFO", "LAPINFO", "TRACKINFO", "PUASFILE",
                  "HISTODATA", "HISTOINFO", "HISTOLAPDATA", "FILEMANAGER", "RUNDATA", "META", "CDCINFO", "CDCDATA",
                  "MATRIXDATA", "LAPDATA"]
        self._init_table(tables)
        print(base + " DATABASE LOADED")

    def variable_list(self,
                      sourcetype=None,
                      run_filter=None,
                      label=None):

        t = self.tables
        var_info = t.VARINFO

        q = self._create_query([var_info.c.Name.label('Name')])
        q = q.distinct()
        q = q.order_by(var_info.c.Name)
        runinfo_col = None
        if sourcetype is None:
            sourcetype = ""

        if sourcetype == "LAPDATA":
            lap_info = t.LAPINFO
            lap_data = t.LAPDATA
            var_info = t.VARINFO
            q = q.join(lap_data, var_info.c.id == lap_data.c.VARINFO_id)
            q = q.join(lap_info, lap_info.c.id == lap_data.c.LAPINFO_id)
            runinfo_col = lap_info.c.RUNINFO_id

        elif sourcetype == "HISTODATA":
            histo_info = t.HISTOINFO
            histo_data = t.HISTODATA
            var_info = t.VARINFO
            q = q.join(histo_info, var_info.c.id == histo_info.c.VARINFO_id)
            q = q.join(histo_data, histo_info.c.id == histo_data.c.HISTOINFO_id)
            runinfo_col = histo_data.c.RUNINFO_id

        if sourcetype != "" and runinfo_col is not None and not run_filter.is_none():
            self.filter_run_from_meta(q, run_filter)
        if label:
            q = q.filter(var_info.c.Name.like(label))

        df = self._read_sql(q)

        return df.Name

    def get_enginetype(self):

        t = self.tables
        file_info = t.FILEINFO

        q = self._create_query([file_info.c.EngineType])
        q = q.distinct()
        q = q.order_by(file_info.c.EngineType)

        df = self._read_sql(q)

        return df

    def get_element(self):

        t = self.tables
        component = t.COMPONENT

        q = self._create_query([component.c.Element])
        q = q.distinct()
        q = q.order_by(component.c.Element)

        df = self._read_sql(q)

        return df

    def get_reference(self,
                      engine_type=None,
                      element=None):

        t = self.tables
        component = t.COMPONENT
        run_info = t.RUNINFO
        file_info = t.FILEINFO
        meta = t.META

        is_component = False
        if element == "Driver":
            q = self._create_query([run_info.c.Driver])
            q = q.join(file_info, run_info.c.FILEINFO_id == file_info.c.id)
            q = q.order_by(run_info.c.Driver)
        elif element == "Event":
            q = self._create_query([file_info.c.Event])
            q = q.order_by(file_info.c.Event)
        else:
            q = self._create_query([component.c.Reference])
            q = q.join(meta, meta.c.COMPONENT_id == component.c.id)
            q = q.join(run_info, meta.c.RUNINFO_id == run_info.c.id)
            q = q.join(file_info, run_info.c.FILEINFO_id == file_info.c.id)
            is_component = True

        if engine_type:
            q = filter_like(q, file_info.c.EngineType, engine_type)
        if is_component:
            q = filter_like(q, component.c.Element, element)
            q = q.order_by(component.c.Reference)
        q = q.distinct()

        df = self._read_sql(q)

        return df

    def _varsubquery(self, VarList):

        t = self.tables
        var_info = t.VARINFO
        select1 = [var_info.c.id]

        q1 = self._create_query(select1)
        q1 = q1.where(var_info.c.Name.in_(VarList))
        q1 = q1.distinct()
        q1 = q1.subquery()

        if len(VarList) == 0:
            q1 = q1.filter(false())

        return q1

    def _runsubquery(self, run_filter):

        t = self.tables
        file_list = t.FILELIST
        run_info = t.RUNINFO
        file_info = t.FILEINFO
        puas_file = t.PUASFILE
        track_info = t.TRACKINFO

        select = [
            run_info.c.id,
            file_list.c.FileName.label('FileName')
        ]
        q = self._create_query(select)
        q = q.join(file_info, file_info.c.id == run_info.c.FILEINFO_id)
        q = q.join(file_list, file_info.c.id == file_list.c.FILEINFO_id)
        q = q.join(puas_file, file_list.c.id == puas_file.c.id)
        q = q.join(track_info, track_info.c.id == run_info.c.TRACKINFO_id)
        q = self.filter_run_from_meta(q, run_filter)
        q = q.distinct()
        q = q.subquery()
        selected = [
            q.c.FileName.label('FileName'),
        ]

        return q, selected

    def get_lapdata(self,
                    VarList=None,
                    run_filter=None,
                    InOutLapFilter=False,
                    DetailedMetadata=False):

        t = self.tables
        lap_info = t.LAPINFO
        lap_data = t.LAPDATA
        run_info = t.RUNINFO
        var_info = t.VARINFO

        if len(VarList) == 0:
            VarList.append("Distance")
        if InOutLapFilter:
            VarList.append("LapType")
        q1 = self._varsubquery(VarList)
        q2, partselect = self._runsubquery(run_filter)
        select3 = [
                      lap_info.c.id,
                      run_info.c.id,
                      lap_info.c.LapNumber.label('LapNumber'),
                      var_info.c.Name.label('Name'),  # as
                      lap_info.c.StartTime.label('LapStartTime'),
                      lap_info.c.EndTime.label('LapEndTime'),
                      lap_data.c.Value.label('Value')
                  ] + partselect

        q3 = self._create_query(select3)
        q3 = q3.join(lap_data, var_info.c.id == lap_data.c.VARINFO_id)
        q3 = q3.join(lap_info, lap_info.c.id == lap_data.c.LAPINFO_id)
        q3 = q3.join(run_info, run_info.c.id == lap_info.c.RUNINFO_id)
        q3 = q3.join(q1, q1.c.id == var_info.c.id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)

        # if run_filter.is_none():
        #     q3 = q3.filter(false())

        df = self._read_sql(q3)

        index = list(df.keys())
        del index[index.index("Value")]
        del index[index.index("Name")]
        del index[index.index("LAPINFO_id")]
        del index[index.index("RUNINFO_id")]

        g = df.pivot_table(index=index, columns=['Name'],
                           values='Value')
        for var in VarList:
            if var not in g:
                g[var] = None

        if InOutLapFilter:
            g["LapType"] = g["LapType"].fillna(2)
            g = g.loc[g["LapType"] == 2]

        g = g.sort_values(by=['LapStartTime'])

        return g

    def get_rundata(self, VarList=None,
                    run_filter=None):

        t = self.tables
        run_info = t.RUNINFO
        var_info = t.VARINFO
        run_data = t.RUNDATA

        q1 = self._varsubquery(VarList)
        q2, partselect = self._runsubquery(run_filter)
        select3 = [var_info.c.Name.label('Name'),  # as,
                   run_data.c.Value.label('Value'),
                   run_info.c.StartTime.label('RunStartTime'),
                   run_info.c.EndTime.label('RunEndTime')
                   ] + partselect

        q3 = self._create_query(select3)
        q3 = q3.join(run_data, var_info.c.id == run_data.c.VARINFO_id)
        q3 = q3.join(run_info, run_info.c.id == run_data.c.RUNINFO_id)
        q3 = q3.join(q1, q1.c.id == var_info.c.id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)

        df = self._read_sql(q3)
        index = list(df.keys())
        del index[index.index("Value")]
        del index[index.index("Name")]

        g = df.pivot_table(index=index, columns=['Name'],
                           values='Value')
        for var in VarList:
            if var not in g:
                g[var] = None

        g = g.sort_values(by=['RunStartTime'])

        return g

    def get_histodata(self, VarList=None,
                      run_filter=None,
                      AgregationFunction=None):

        t = self.tables
        run_info = t.RUNINFO
        var_info = t.VARINFO

        histo_info = t.HISTOINFO
        histo_data = t.HISTODATA

        if AgregationFunction == "Min":
            ag_fun = func.min
        elif AgregationFunction == "Max":
            ag_fun = func.max
        elif AgregationFunction == "Sum":
            ag_fun = func.sum
        elif AgregationFunction == "Mean":
            ag_fun = func.avg

        q1 = self._varsubquery(VarList)
        q2, partselect = self._runsubquery(run_filter)
        select3 = [var_info.c.Name.label('Name'),
                   ag_fun(histo_data.c.Value.label('Value')),
                   histo_info.c.LeftEdge.label('LeftEdge'),
                   histo_info.c.RightEdge.label('RightEdge')]

        q3 = self._create_query(select3)
        q3 = q3.join(histo_info, var_info.c.id == histo_info.c.VARINFO_id)
        q3 = q3.join(histo_data, histo_info.c.id == histo_data.c.HISTOINFO_id)
        q3 = q3.join(run_info, run_info.c.id == histo_data.c.RUNINFO_id)
        q3 = q3.join(q1, q1.c.id == var_info.c.id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)

        q3 = q3.group_by(histo_info.c.LeftEdge, histo_info.c.RightEdge, var_info.c.Name)
        q3 = q3.order_by(var_info.c.Name, histo_info.c.LeftEdge, histo_info.c.RightEdge)

        df = self._read_sql(q3)
        return df

    def get_histolapdata(self,
                         VarList=None,
                         run_filter=None,
                         AgregationFunction=None,
                         InOutLapFilter=False):

        t = self.tables
        run_info = t.RUNINFO
        var_info = t.VARINFO
        histo_info = t.HISTOINFO
        histolap_data = t.HISTOLAPDATA
        lap_info = t.LAPINFO

        if AgregationFunction == "Min":
            ag_fun = func.min
        elif AgregationFunction == "Max":
            ag_fun = func.max
        elif AgregationFunction == "Sum":
            ag_fun = func.sum
        elif AgregationFunction == "Mean":
            ag_fun = func.avg

        q1 = self._varsubquery(VarList)
        q2, partselect = self._runsubquery(run_filter)
        select3 = [var_info.c.Name.label('Name'),
                   ag_fun(histolap_data.c.Value.label('Value')),
                   histo_info.c.LeftEdge.label('LeftEdge'),
                   histo_info.c.RightEdge.label('RightEdge')]
        q3 = self._create_query(select3)
        q3 = q3.join(histo_info, var_info.c.id == histo_info.c.VARINFO_id)
        q3 = q3.join(histolap_data, histo_info.c.id == histolap_data.c.HISTOINFO_id)
        q3 = q3.join(lap_info, lap_info.c.id == histolap_data.c.LAPINFO_id)
        q3 = q3.join(run_info, run_info.c.id == lap_info.c.RUNINFO_id)
        q3 = q3.join(q1, q1.c.id == var_info.c.id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)

        q3 = q3.group_by(histo_info.c.LeftEdge, histo_info.c.RightEdge, var_info.c.Name)
        q3 = q3.order_by(var_info.c.Name, histo_info.c.LeftEdge, histo_info.c.RightEdge)

        g = self._read_sql(q3)

        return g

    def get_cdc(self, run_filter=None, cdc_id=None):

        t = self.tables
        run_info = t.RUNINFO
        cdc_info = t.CDCINFO
        cdc_data = t.CDCDATA

        q2, partselect = self._runsubquery(run_filter)
        select3 = [cdc_info.c.id.label('cdcId'),
                   cdc_info.c.Category.label('Category'),
                   cdc_info.c.Description.label('Description'),
                   cdc_info.c.Channel.label('Channel'),
                   cdc_info.c.Criterion.label('Criterion'),
                   cdc_info.c.Value.label('Value'),
                   cdc_info.c.Unit.label('Unit'),
                   cdc_info.c.Occurrences.label('Occurrences'),
                   cdc_info.c.Duration.label('Duration'),
                   func.sum(cdc_data.c.Occurrences).label('TotalOcc'),
                   func.sum(cdc_data.c.Duration).label('TotalDur')]

        q3 = self._create_query(select3)
        q3 = q3.join(run_info, run_info.c.id == cdc_data.c.RUNINFO_id)
        q3 = q3.join(cdc_info, cdc_info.c.id == cdc_data.c.CDCINFO_id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)

        q3 = q3.group_by(cdc_info.c.Category, cdc_info.c.Description, cdc_info.c.Channel, cdc_info.c.Unit,
                         cdc_info.c.Criterion, cdc_info.c.Value, cdc_info.c.Occurrences, cdc_info.c.Duration,
                         cdc_info.c.id)
        q3 = q3.order_by(cdc_info.c.Category, cdc_info.c.Description, cdc_info.c.Channel, cdc_info.c.Unit,
                         cdc_info.c.Criterion, cdc_info.c.Value, cdc_info.c.Occurrences, cdc_info.c.Duration,
                         cdc_info.c.id)
        q3 = filter_eq(q3, cdc_info.c.id, cdc_id)
        g = self._read_sql(q3)
        return g

    def get_cdcdata(self, run_filter=None, cdc_id=None, var=None):

        t = self.tables
        run_info = t.RUNINFO
        cdc_info = t.CDCINFO
        cdc_data = t.CDCDATA
        run_data = t.RUNDATA
        var_info = t.VARINFO

        if not var:
            var = "Distance"

        q1 = self._varsubquery([var])
        q2, partselect = self._runsubquery(run_filter)
        select3 = [cdc_data.c.Occurrences.label('Occurrences'),
                   cdc_data.c.Duration.label('Duration'),
                   run_info.c.StartTime.label('RunStartTime'),
                   run_info.c.EndTime.label('RunEndTime'),
                   var_info.c.Name.label("Name"),
                   run_data.c.Value.label("Value")
                   ] + partselect
        q3 = self._create_query(select3)
        q3 = q3.join(run_info, run_info.c.id == cdc_data.c.RUNINFO_id)
        q3 = q3.join(run_data, run_info.c.id == run_data.c.RUNINFO_id)
        q3 = q3.join(var_info, var_info.c.id == run_data.c.VARINFO_id)
        q3 = q3.join(cdc_info, cdc_info.c.id == cdc_data.c.CDCINFO_id)
        q3 = q3.join(q1, q1.c.id == var_info.c.id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)
        q3 = filter_eq(q3, cdc_info.c.id, [cdc_id])
        q3 = q3.order_by(run_info.c.StartTime)
        q3 = q3.distinct()

        g = self._read_sql(q3)

        return g

    def get_matrixdata(self, VarList=None,
                       run_filter=None,
                       aggregation_function=None):
        t = self.tables
        run_info = t.RUNINFO
        var_info = t.VARINFO
        matrix_data = t.MATRIXDATA

        q1 = self._varsubquery(VarList)
        q2, partselect = self._runsubquery(run_filter)
        select3 = [var_info.c.Name.label("Name"),
                   matrix_data.c.Value.label("Value"),
                   run_info.c.StartTime.label('RunStartTime'),
                   run_info.c.EndTime.label('RunEndTime')
                   ] + partselect

        q3 = self._create_query(select3)
        q3 = q3.join(matrix_data, var_info.c.id == matrix_data.c.VARINFO_id)
        q3 = q3.join(run_info, run_info.c.id == matrix_data.c.RUNINFO_id)
        q3 = q3.join(q1, q1.c.id == var_info.c.id)
        q3 = q3.join(q2, q2.c.id == run_info.c.id)
        q3 = q3.distinct()
        q3 = q3.order_by(run_info.c.StartTime)

        df = self._read_sql(q3)

        for i in range(len(df)):
            js = json.loads("[" + df.Value[i].replace(";", "],[").replace(" ", ",") + "]")
            arr = np.array(js, dtype=np.float64)
            df.Value[i] = arr

            # df.loc[i,"Value"] = arr

        index = list(df.keys())
        del index[index.index("Value")]
        del index[index.index("Name")]

        g = df.pivot_table(index=index, columns=['Name'],
                           values='Value')
        for var in VarList:
            if var not in g:
                g[var] = None

        return g

    def run_get(self, run_filter=None, component_meta=False):

        t = self.tables
        file_info = t.FILEINFO
        file_list = t.FILELIST
        run_info = t.RUNINFO
        track_info = t.TRACKINFO
        puas_file = t.PUASFILE
        component = t.COMPONENT
        meta = t.META

        select = [file_info.c.EngineType.label("EngineType"),
                  file_info.c.NPUReference.label("NPUReference"),
                  puas_file.c.Chassis.label("Chassis"),
                  puas_file.c.ChassisNumber.label("ChassisNumber"),
                  track_info.c.Track.label("Circuit"),
                  run_info.c.Driver.label("Driver"),
                  file_info.c.Event.label("Event"),
                  puas_file.c.Session.label("Session"),
                  puas_file.c.RunType.label("RunType"),
                  puas_file.c.RunNumber.label("RunNumber"),
                  puas_file.c.FileName.label("FileName"),
                  run_info.c.StartTime.label("RunStartTime"),
                  run_info.c.EndTime.label("RunEndTime"),
                  run_info.c.NbLaps.label("NbLaps"),
                  file_list.c.SessionName.label("SessionName"),
                  file_list.c.Folder.label("Folder"),
                  file_list.c.SessionDescription.label("SessionDescription"),
                  file_info.c.Type.label("Type"),
                  puas_file.c.FileName.label("Identifier")
                  ]

        if run_filter.Component:
            pass

        query2 = self._create_query(select)
        query2 = query2.join(track_info, track_info.c.id == run_info.c.TRACKINFO_id)
        query2 = query2.join(file_info, file_info.c.id == run_info.c.FILEINFO_id)
        query2 = query2.join(file_list, file_list.c.FILEINFO_id == file_info.c.id)
        query2 = query2.join(puas_file, file_list.c.id == puas_file.c.id, )
        query2 = query2.filter(puas_file.c.id.isnot(None))
        query2 = query2.distinct()

        query2 = self.filter_run_from_meta(query2, run_filter)
        df = self._read_sql(query2)

        if component_meta:
            select = [puas_file.c.FileName.label("FileName"),
                      component.c.Element.label('Element'),
                      component.c.Reference.label('Reference')
                      ]
            query3 = self._create_query(select)
            query3 = query3.join(meta, meta.c.COMPONENT_id == component.c.id)
            query3 = query3.join(run_info, meta.c.RUNINFO_id == run_info.c.id)
            query3 = query3.join(file_info, file_info.c.id == run_info.c.FILEINFO_id)
            query3 = query3.join(file_list, file_list.c.FILEINFO_id == file_info.c.id)
            query3 = query3.join(puas_file, file_list.c.id == puas_file.c.id)
            query3 = self.filter_run_from_meta(query3, run_filter)
            df2 = self._read_sql(query3)

            g = df2.pivot_table(index='FileName', columns=['Element'],
                                values='Reference', aggfunc=lambda x: ' '.join(x))
            df = df.join(g, on='FileName')

        return df

    def filter_run_from_meta(self, q, run_filter=None):

        t = self.tables
        file_info = t.FILEINFO
        run_info = t.RUNINFO
        track_info = t.TRACKINFO
        puas_file = t.PUASFILE
        file_list = t.FILELIST
        components = t.COMPONENT
        meta = t.META

        q = filter_eq(q, file_info.c.EngineType, run_filter.EngineType)
        q = filter_eq(q, file_info.c.NPUReference, run_filter.NPUReference)
        q = filter_eq(q, puas_file.c.Chassis, run_filter.Chassis)
        q = filter_eq(q, puas_file.c.ChassisNumber, run_filter.ChassisNumber)
        q = filter_eq(q, run_info.c.Driver, run_filter.Driver)
        q = filter_eq(q, puas_file.c.Event, run_filter.Event)
        q = filter_eq(q, puas_file.c.Session, run_filter.Session)
        q = filter_eq(q, puas_file.c.RunType, run_filter.RunType)
        q = filter_eq(q, puas_file.c.RunNumber, run_filter.RunNumber)
        q = filter_eq(q, puas_file.c.FileName, run_filter.FileName)
        q = filter_eq(q, file_info.c.Type, run_filter.Type)
        q = filter_eq(q, track_info.c.Track, run_filter.Circuit)
        q = filter_like(q, file_list.c.SessionDescription, run_filter.SessionDescription)
        q = filter_like(q, file_list.c.SessionName, run_filter.SessionName)

        if run_filter.Component:
            subq = self._create_query([meta.c.RUNINFO_id])
            subq = subq.join(components, meta.c.COMPONENT_id == components.c.id)
            subq = filter_eq(subq, components.c.Reference, run_filter.Component)
            subq = subq.subquery()

            q = q.join(subq, subq.c.RUNINFO_id == run_info.c.id)

        q = q.distinct()

        return q


async def get_token_header(x_token: str = Header()):
    if x_token != "FastAPI_token":
        raise HTTPException(status_code=400, detail="FastAPI header invalid")


class RUNFILTER():
    """
    class that permits to filtre runs
    """
    keys = ["Chassis",
            "ChassisNumber",
            "EngineType",
            "NPUReference",
            "Component",
            "Circuit",
            "Driver",
            "Event",
            "Session",
            "RunType",
            "RunNumber",
            "FileName",
            "SessionName",
            "SessionDescription",
            "Type",
            "Competition"]

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
        if not isinstance(value, type([])) and value is not None:
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
