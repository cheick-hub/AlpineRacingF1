from enum import Enum
from typing import List

from fastapi import APIRouter, Response, Query
from pydantic import BaseModel

from bdd.PUAS import PUAS3, RUNFILTER

router = APIRouter(
    prefix="/run",
    tags=["run"],
    responses={404: {"description": "Not found"}},
)
#
PUAS3("PUAS3")


class CompetitionEnum(str, Enum):
    PUAS3 = "F1"
    CATANA_FE = "FE"


class RUNMETA(BaseModel):
    Chassis: List[str] | None = None
    ChassisNumber: List[str] | None = None
    EngineType: List[str] | None = None
    NPUReference: List[str] | None = None
    Component: List[str] | None = None
    Circuit: List[str] | None = None
    Driver: List[str] | None = None
    Event: List[str] | None = None
    Session: List[str] | None = None
    RunType: List[str] | None = None
    RunNumber: List[int] | None = None
    FileName: List[str] | None = None
    SessionName: List[str] | None = None
    SessionDescription: List[str] | None = None
    Type: List[str] | None = None
    Competition: CompetitionEnum


# PUAS3("CATANA_FE")


@router.get("/")
async def run(
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),
        DetailedMetadata: bool = Query(default=True)):  #
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())

    p = PUAS3(Competition.name)
    return Response(p.run_get(run_filter, DetailedMetadata).to_json(orient="split", date_format="iso"),
                    media_type="application/json")

class RunAddModel(BaseModel):
    DetailedMetadata: bool = False

class RunModel(RUNMETA, RunAddModel):
    pass

@router.post("/")
async def runpost(runmodel: RunModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(runmodel.__dict__)

    p = PUAS3(runmodel.Competition.name)
    return Response(p.run_get(run_filter, runmodel.DetailedMetadata).to_json(orient="split", date_format="iso"),
                    media_type="application/json")


class SourcesEnum(str, Enum):
    LAPDATA = "LAPDATA"
    HISTODATA = "HISTODATA"


@router.get("/variablelist")
async def variable_list(
        Source: SourcesEnum = Query(default=""),
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        Name: str = Query(default="%")):
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(p.variable_list(Source, run_filter, Name).to_json(orient="values", date_format="iso"),
                    media_type="application/json")


@router.get("/lapdata")
async def lapdata(
        Variables: List[str] = Query(default=[]),
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        InOutLapFilter: bool = Query(default=False)):
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(
        p.get_lapdata(Variables, run_filter, InOutLapFilter).reset_index().to_json(orient='split',
                                                                                   date_format="iso"),
        media_type="application/json")

class LapDataAddModel(BaseModel):
    Variables: List[str] | None = None
    InOutLapFilter: bool = False

class LapDataModel(RUNMETA, LapDataAddModel):
    pass

@router.post("/lapdata")
async def lapdatapost(lapdata: LapDataModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(lapdata.__dict__)

    p = PUAS3(lapdata.Competition.name)
    return Response(
        p.get_lapdata(lapdata.Variables, run_filter, lapdata.InOutLapFilter).reset_index().to_json(orient='split',
                                                                                   date_format="iso"),
        media_type="application/json")


@router.get("/rundata")
async def rundata(
        Variables: List[str] = Query(default=[]),
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1")):  # run_filter

    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(
        p.get_rundata(Variables, run_filter).reset_index().to_json(orient='split', date_format="iso"),
        media_type="application/json")

class RunDataAddModel(BaseModel):
    Variables: List[str] | None = None

class RunDataModel(RUNMETA, RunDataAddModel):
    pass


@router.post("/rundata")
async def rundatapost(rundatamodel: RunDataModel,
                      ):
    run_filter = RUNFILTER()
    run_filter.from_dict(rundatamodel.__dict__)

    p = PUAS3(rundatamodel.Competition.name)
    return Response(
        p.get_rundata(rundatamodel.Variables, run_filter).reset_index().to_json(orient='split', date_format="iso"),
        media_type="application/json")


class aggregationfunctionEnum(str, Enum):
    Sum = "Sum"
    Max = "Max"
    Min = "Min"
    Mean = "Mean"


@router.get("/histodata")
async def histodata(
        Variables: List[str] = Query(default=[]),
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        Type: List[str] = Query(default=[]),
        AgregationFunction: aggregationfunctionEnum = Query(default="Sum"), ):  # run_filter

    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(
        p.get_histodata(Variables, run_filter, AgregationFunction).reset_index().to_json(orient='split',
                                                                                         date_format="iso"),
        media_type="application/json")

class HistodataAddModel(BaseModel):
    Variables: List[str] | None = None
    AgregationFunction: aggregationfunctionEnum = aggregationfunctionEnum.Sum

class HistodataModel(RUNMETA, HistodataAddModel):
    pass
    
@router.post("/histodata")
async def histodatapost(histomodel: HistodataModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(histomodel.__dict__)

    p = PUAS3(histomodel.Competition.name)
    return Response(
        p.get_histodata(histomodel.Variables, run_filter, histomodel.AgregationFunction).reset_index().to_json(orient='split',
                                                                                         date_format="iso"),
        media_type="application/json")


@router.get("/histolapdata")
async def histolapdata(
        Variables: List[str] = Query(default=[]),
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        AgregationFunction: aggregationfunctionEnum = Query(default="Sum"),
        InOutLapFilter: bool = Query(default=False)):
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(
        p.get_histolapdata(Variables, run_filter, AgregationFunction, InOutLapFilter).reset_index().to_json(
            orient='split',
            date_format="iso"),
        media_type="application/json")

class HistolapAddModel(BaseModel):
    Variables: List[str] | None = None
    AgregationFunction: aggregationfunctionEnum = aggregationfunctionEnum.Sum
    InOutLapFilter: bool = False

class HistolapModel(RUNMETA, HistolapAddModel):
    pass

@router.post("/histolapdata")
async def histolapdatapost(histolap: HistolapModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(histolap.__dict__)

    p = PUAS3(histolap.Competition.name)
    return Response(
        p.get_histolapdata(histolap.Variables, run_filter, histolap.AgregationFunction, histolap.InOutLapFilter).reset_index().to_json(
            orient='split',
            date_format="iso"),
        media_type="application/json")


@router.get("/cdcinfo")
async def cdcinfo(
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        cdcId: List[int] = Query(default=[]),
        Type: List[str] = Query(default=[])):  # run_filter

    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(
        p.get_cdc(run_filter, cdcId).reset_index().to_json(orient='split', date_format="iso"),
        media_type="application/json")

class CdcinfoAddModel(BaseModel):
    cdcId: List[int] = []

class CdcinfoModel(RUNMETA, CdcinfoAddModel):
    pass

@router.post("/cdcinfo")
async def cdcinfopost(cdcinfo: CdcinfoModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(cdcinfo.__dict__)

    p = PUAS3(cdcinfo.Competition.name)
    return Response(
        p.get_cdc(run_filter, CdcinfoModel.cdcId).reset_index().to_json(orient='split', date_format="iso"),
        media_type="application/json")


@router.get("/cdcdata")
async def get_cdcdata(
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        cdcId: int = Query(default=[]),
        Variable: str = Query(default=[])):
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(
        p.get_cdcdata(run_filter, cdcId, Variable).reset_index().to_json(orient='split', date_format="iso"),
        media_type="application/json")

class CdcdataAddModel(BaseModel):
    Variables: List[str] | None = None
    cdcId: List[int] = []

class CdcdataModel(RUNMETA, CdcdataAddModel):
    pass

@router.post("/cdcdata")
async def cdcdatapost(cdcdata: CdcdataModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(cdcdata.__dict__)

    p = PUAS3(cdcdata.Competition.name)
    return Response(
        p.get_cdcdata(run_filter, cdcdata.cdcId, cdcdata.Variables).reset_index().to_json(orient='split', date_format="iso"),
        media_type="application/json")


@router.get("/matrixdata")
async def get_matrixdata(
        Chassis: List[str] = Query(default=[]),  # run_filter
        ChassisNumber: List[str] = Query(default=[]),  # run_filter
        EngineType: List[str] = Query(default=[]),  # run_filter
        NPUReference: List[str] = Query(default=[]),  # run_filter
        Component: List[str] = Query(default=[]),  # run_filter
        Circuit: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        FileName: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        Type: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),  # run_filter
        Variables: List[str] = Query(default=[])):
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())
    p = PUAS3(Competition.name)
    return Response(p.get_matrixdata(Variables, run_filter).reset_index().to_json(orient="split", date_format="iso"),
                    media_type="application/json")

class MatrixdataAddModel(BaseModel):
    Variables: List[str] | None = None

class MatrixdataModel(RUNMETA, MatrixdataAddModel):
    pass

@router.post("/matrixdata")
async def matrixdatapost(matrixdata: MatrixdataModel):
    run_filter = RUNFILTER()
    run_filter.from_dict(matrixdata.__dict__)

    p = PUAS3(matrixdata.Competition.name)
    return Response(p.get_matrixdata(matrixdata.Variables, run_filter).reset_index().to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/enginetype")
async def get_enginetype(Competition: CompetitionEnum = Query(default="F1")):
    p = PUAS3(Competition.name)
    return Response(p.get_enginetype().to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/element")
async def get_element(Competition: CompetitionEnum = Query(default="F1")):
    p = PUAS3(Competition.name)
    return Response(p.get_element().to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/reference")
async def get_reference(EngineType: List[str] = Query(default=[]),
                        Element: List[str] = Query(default=[]),
                        Competition: CompetitionEnum = Query(default="F1")):
    p = PUAS3(Competition.name)
    return Response(p.get_reference(EngineType, Element).to_json(orient="split", date_format="iso"),
                    media_type="application/json")
