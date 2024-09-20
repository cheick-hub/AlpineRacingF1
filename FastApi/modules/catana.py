from enum import Enum
from functools import wraps
import json
import logging
import time
from typing import List, Dict
import sys

from fastapi import APIRouter, Response, Request, Query
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
from pydantic import BaseModel, Field

from bdd.CATANA import CATANA, RUNFILTER
from parquet.CatanaAggregationEnum import CatanaAggregationEnum
from parquet.PARQUET import CatanaDataTypeEnum
from parquet.LapData import LapData
from parquet.HistoData import HistoData
from parquet.HistoLapData import HistoLapData
from parquet.OtherData import OtherData
from parquet.RunData import RunData
from parquet.CDCData import CDCData
from parquet.Histo2DData import Histo2DData
from parquet.Channel import ChannelData

logger = logging.getLogger('main_log')

router = APIRouter(
    prefix="/catana",
    tags=["catana"],
    responses={404: {"description": "Not found"}},
)
#
CATANA()


class CompetitionEnum(str, Enum):
    PUAS3 = "F1"
    F1LIVE = "F1 LIVE"
    CATANA_FE = "FE"
    LMDh = "LMDh"

class SourceEnum(str, Enum):
    PUAS3 = "F1"
    LIVE = "F1 Live"
    CATANA_FE = "FE"
    LMDh = "LMDh"

class ListElements(BaseModel):
    Competition: CompetitionEnum = "F1"
    Element : str = "EngineType"
    FilterElements : str|list[str] = None
    FilterValues : str|list[str] = None
    OnlyRunProcessed : bool = False

class CATANADATA(BaseModel):
    Competition: CompetitionEnum | SourceEnum
    Variables: list[str] | str
    RunUID: list[str] | str
    Years: list[int] | int
    Update: bool = Field(default=False, include_in_schema=False)

class AGGCATANA(CATANADATA):
    AggregationFunction: list[CatanaAggregationEnum] | CatanaAggregationEnum



class LAPMETA(BaseModel):
    RunUID: list[str] | str


class RUNMETA(BaseModel):
    # Chassis: List[str] | None | str = None
    # Car: List[str] | None | str = None
    # ChassisNumber: List[str] | None | str = None
    EngineType: List[str] | None | str = None
    # PU: List[str] | None | str = None
    Component: Dict[str, str] | None
    Track: List[str] | None | str = None
    Driver: List[str] | None | str = None
    Event: List[str] | None | str = None
    Session: List[str] | None | str = None
    RunType: List[str] | None | str = None
    RunNumber: List[int] | None | int = None
    RunUID: List[str] | None | str = None
    # SessionName: List[str] | None | str = None
    SessionDescription: List[str] | None | str = None
    # Type: List[str] | None | str = None
    RunTag: List[str] | None | str = None
    Competition: CompetitionEnum
    DetailedMetadata: bool = False

class VARIABLES(BaseModel):
    Competition: CompetitionEnum
    DataType: CatanaDataTypeEnum
    RunUID: list[str] | str
    Years: list[int] | int


def parse_df(df:pd.DataFrame) -> dict:
    """
        Function to parse a dataframe from PARQUET in the correct orient
        without using the (slower) build-in functions.
    """
    res = {}
    res['columns'] = list(df.columns)
    res['index'] = list(range(df.shape[0]))
    res['data'] = df.values.tolist()
    return res

def json_dumps_iterator(large_dict):
    """
    Itère sur un très grand dictionnaire et renvoie petit à petit
    des fragments de la version JSON sérialisée.

    :param large_dict: Dictionnaire à sérialiser en JSON.
    :return: Un itérateur qui renvoie des fragments JSON.
    """

    # Commence avec la première accolade ouvrante '{'
    yield '{'

    # Itère sur les clés/valeurs du dictionnaire
    iterator = iter(large_dict.items())
    first_item = True

    for key, value in iterator:
        if not first_item:
            # Ajoute une virgule avant chaque nouvel élément sauf le premier
            yield ','
        first_item = False

        # Sérialiser la clé et la valeur en JSON
        key_json = json.dumps(key)

        nelement_max = 100000
        if isinstance(value, dict):
            yield f"{key_json}:"
            for val in json_dumps_iterator(value):
                yield val
        elif isinstance(value, list):

            yield f"{key_json}:"
            yield '['
            for i in range(0, len(value), nelement_max):
                yield json.dumps(value[i:i + nelement_max])[1:-1]
                if i + nelement_max < len(value):
                    yield ", "
                # print(value[i:i+5])

            yield ']'
        else:
            value_json = json.dumps(value)
            yield f"{key_json}: {value_json}"

        # value_json = json.dumps(value)

        # Renvoyer le fragment JSON clé:valeur
        # yield f"{key_json}: {value_json}"

    # Termine avec l'accolade fermante '}'
    yield '}'
 
def logger_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        ip = kwargs['request'].client.host
        func_name = func.__name__
        logger.debug(f' {ip} | Starting {func_name}')
        start = time.time()
        my_result = func(*args, **kwargs)
        end = time.time() - start
        max_tolerated_size, res_size = 50 * 1e6, sys.getsizeof(my_result.body)
        if res_size > max_tolerated_size:
            logger.warning(f"""{ip} | {func_name} | Response body is """
                           f"""{res_size/1e6:.3f}Mo""")
        logger.info(f' {ip} | {func_name} done [{end:.3f}s]\n')
        return my_result
    return wrapper

@router.post("/")
@logger_decorator
def run_post(request: Request,
             runmeta: RUNMETA):
    run_filter = RUNFILTER()
    run_filter.from_dict(runmeta.__dict__)

    p = CATANA()
    result = p.run_get(run_filter, runmeta.DetailedMetadata)
    return Response(result.to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/")
@logger_decorator
def run(request: Request,
        EngineType: List[str] = Query(default=[]),  # run_filter
        Track: List[str] = Query(default=[]),  # run_filter
        Driver: List[str] = Query(default=[]),  # run_filter
        Event: List[str] = Query(default=[]),  # run_filter
        Session: List[str] = Query(default=[]),  # run_filter
        RunType: List[str] = Query(default=[]),  # run_filter
        RunNumber: List[int] = Query(default=[]),  # run_filter
        RunUID: List[str] = Query(default=[]),  # run_filter
        SessionName: List[str] = Query(default=[]),  # run_filter
        SessionDescription: List[str] = Query(default=[]),  # run_filter
        RunTag: List[str] = Query(default=[]),  # run_filter
        Competition: CompetitionEnum = Query(default="F1"),
        DetailedMetadata: bool = Query(default=False)):  #
    run_filter = RUNFILTER()
    run_filter.from_dict(locals().copy())

    p = CATANA()
    result = p.run_get(run_filter, DetailedMetadata)
    return Response(result.to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/list_element")
@logger_decorator
def list_element(request: Request,
                 Competition: SourceEnum = Query(default="F1")):
    p = CATANA()
    result = p.list_element(Competition)
    return Response(result.to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/list_value")
@logger_decorator
def get_list_value(request: Request,
                   Competition: SourceEnum = Query(default="F1"),
                   Element: str = Query(default='EngineType'),
                   FilterElement: List[str] = Query(default=[]),
                   FilterValue: List[str] = Query(default=[]),
                   OnlyRunProcessed: bool = Query(default=False)):
    p = CATANA()
    result = p.list_value(Competition, Element, FilterElement, FilterValue,
                          OnlyRunProcessed)
    r = result.to_json(orient="split", date_format="iso")
    return Response(r, media_type="application/json")

@router.post("/list_value")
@logger_decorator
def post_list_value(request: Request,
                    data : ListElements):
    p = CATANA()
    result = p.list_value(data.Competition, data.Element,
                          data.FilterElements, data.FilterValues,
                          data.OnlyRunProcessed)
    
    r = result.to_json(orient="split", date_format="iso")
    return Response(r, media_type="application/json")


@router.get("/list_cdc")
def get_list_cdc(
        Competition: CompetitionEnum = Query(default="F1"),
        EngineType: str = Query(default=None),
        RunTag: str = Query(default=None)):
    p = CATANA()
    result = p.cdc_list(Competition, EngineType, RunTag)
    return Response(result.to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/list_laptype")
def list_laptype():
    p = CATANA()
    result = p.laptype_get()
    r = result.to_json(orient="split", date_format="iso")
    return Response(r, media_type="application/json")


@router.get("/cdc")
@logger_decorator
def get_cdc(request: Request, CDCListUID: List[str] = Query(default=[]),
            CDCUID: List[str] = Query(default=[]),
            Identifier: List[str] = Query(default=[]),
            CDCLimitUID: List[str] = Query(default=[])):
    p = CATANA()
    CDCListUID = list(map(lambda x: x.upper(), CDCListUID))
    CDCUID = list(map(lambda x: x.upper(), CDCUID))
    CDCLimitUID = list(map(lambda x: x.upper(), CDCLimitUID))
    result = p.cdc_get(CDCListUID, CDCUID, CDCLimitUID, Identifier)
    return Response(result.to_json(orient="split", date_format="iso"),
                    media_type="application/json")


# @router.get('/cdc_uid_from_identifiers')
# async def get_cdc_uids(Identifiers: List[str] = Query(default=[]),
#                        EngineType: str = Query(default='RE24A'),
#                        RunTag: str = Query(default='Track'),
#                        Competition: CompetitionEnum = Query(
#                            default=CompetitionEnum.PUAS3),
#                        RunDate: str = Query(default=[])):
#     p = CATANA()
#     result = p.get_cdcuid_from_identifiers(
#         Identifiers, EngineType, RunTag, Competition, RunDate)
#     return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
#                     media_type="application/json")

@router.get('/get_run_cdcinfo')
@logger_decorator
def get_run_cdcinfo(request: Request,
                    Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                    Variables: List[str] = Query(default=[]),
                    RunUID: List[str] = Query(default=[]),
                    Years: List[int] = Query(default=[])):
    p = CDCData(Competition, Variables, RunUID, Years)
    result = p.get_CDCINFO_from_parquet()
    res = {k: json.loads(v.to_json(orient="split", date_format="iso",
                                   default_handler=str)) for k, v in result.items()}
    return Response(json.dumps(res),
                    media_type="application/json")


@router.post('/get_run_cdcinfo')
@logger_decorator
def post_run_cdcinfo(request: Request, data: CATANADATA):
    p = CDCData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.get_CDCINFO_from_parquet()
    res = {k: json.loads(v.to_json(orient="split", date_format="iso",
                                   default_handler=str)) for k, v in result.items()}
    return Response(json.dumps(res),
                    media_type="application/json")


@router.get("/alias")
@logger_decorator
def get_alias(request: Request, BeginDate: str = Query(default="2000"),
              EndDate: str = Query(default="3000"),
              Competition: CompetitionEnum = Query(default="F1"),
              Type: str = Query(default="")):
    p = CATANA()

    result = p.alias_get(BeginDate, EndDate, Competition, Type)
    return Response(result.to_json(orient="split", date_format="iso"),
                    media_type="application/json")


@router.get("/lap_metadata")
@logger_decorator
def get_lap_meta(request: Request, RunUID: List[str] | str = Query(default="")):
    p = CATANA()
    result = p.lap_meta_get(RunUID)
    return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
                    media_type="application/json")


@router.post("/lap_metadata")
@logger_decorator
def post_lap_meta(request: Request, RunUID: LAPMETA):
    p = CATANA()
    result = p.lap_meta_get(RunUID.RunUID)
    return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
                    media_type="application/json")


# Parquets endpoints


@router.get("/get_cdcdata")
@logger_decorator
def get_cdcdata(request: Request,
                Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                Variables: List[str] = Query(default=[]),
                AggregationFunction: List[CatanaAggregationEnum] = Query(default=[CatanaAggregationEnum.SUM]),
                RunUID: List[str] = Query(default=[]),
                Years: List[int] = Query(default=[]),
                Update: bool = Query(default=False, include_in_schema=True)):
    p = CDCData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update, AggregationFunction)
    cdc_info = {k: parse_df(v) for k, v in result[0].items()}
    cdc_data = result[1].to_json(orient="split", date_format="iso",
                            default_handler=str)
    res = json.dumps({'cdc_info': cdc_info, 'cdc_data': cdc_data})
    return Response(res, media_type="application/json")

@router.post("/get_cdcdata")
@logger_decorator
def post_cdcdata(request: Request, data: AGGCATANA):
    p = CDCData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update, data.AggregationFunction)
    cdc_info = {k: json.loads(v.to_json(orient="split", date_format="iso",
                                   default_handler=str)) for k, v in result[0].items()}
    cdc_data = result[1].to_json(orient="split", date_format="iso",
                            default_handler=str)
    res = json.dumps({'cdc_info': cdc_info, 'cdc_data': json.loads(cdc_data)})
    return Response(res, media_type="application/json")

@router.get("/get_histo2ddata")
@logger_decorator
def get_histo2ddata(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                    Variables: List[str] = Query(default=[]),
                    AggregationFunction : List[CatanaAggregationEnum] = Query(default=[CatanaAggregationEnum.SUM]),
                    RunUID: List[str] = Query(default=[]),
                    Years: List[int] = Query(default=[]),
                    Update: bool = Query(default=False, include_in_schema=True)):
    p = Histo2DData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update, AggregationFunction)
    res = {k: parse_df(v) for k, v in result.items()}
    return Response(json.dumps(res), media_type="application/json")


@router.post("/get_histo2ddata")
@logger_decorator
def post_histo2ddata(request: Request, data: AGGCATANA):
    p = Histo2DData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update, data.AggregationFunction)
    res = {k: parse_df(v) for k, v in result.items()}
    res = json.dumps(res)
    return Response(res, media_type="application/json")


@router.get("/get_histodata")
@logger_decorator
def get_histodata(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                  Variables: List[str] = Query(default=[]),
                  AggregationFunction: List[CatanaAggregationEnum] = Query(default=[CatanaAggregationEnum.SUM]),
                  RunUID: List[str] = Query(default=[]),
                  Years: List[int] = Query(default=[]),
                  Update: bool = Query(default=False, include_in_schema=True)):
    p = HistoData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update, AggregationFunction)
    res = {k: parse_df(v) for k, v in result.items()}
    return Response(json.dumps(res), media_type="application/json")


@router.post("/get_histodata")
@logger_decorator
def post_histodata(request: Request, data: AGGCATANA):
    p = HistoData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update, data.AggregationFunction)
    res = {k: parse_df(v) for k, v in result.items()}
    return Response(json.dumps(res), media_type="application/json")


@router.get("/get_histolapdata")
@logger_decorator
def get_histolapdata(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                     Variables: List[str] = Query(default=[]),
                     AggregationFunction: List[CatanaAggregationEnum] = Query(default=[CatanaAggregationEnum.SUM]),
                     RunUID: List[str] = Query(default=[]),
                     Years: List[int] = Query(default=[]),
                     Update: bool = Query(default=False, include_in_schema=True)):
    p = HistoLapData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update, AggregationFunction)
    res = {k: parse_df(v) for k, v in result.items()}
    return Response(json.dumps(res), media_type="application/json")


@router.post("/get_histolapdata")
@logger_decorator
def post_histolapdata(request: Request, data: AGGCATANA):
    p = HistoLapData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update, data.AggregationFunction)
    res = {k: parse_df(v) for k, v in result.items()}
    res = json.dumps(res)
    # res_size = sys.getsizeof(res)
    # if res_size > 100 * 1e6:
    #     logger.warning(f"Response size is {res_size / 1e6:.2f}Mo")


    return Response(res, media_type="application/json")


@router.get("/get_lapdata")
@logger_decorator
def get_lapdata(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                Variables: List[str] = Query(default=[]),
                RunUID: List[str] = Query(default=[]),
                Years: List[int] = Query(default=[]),
                Update: bool = Query(default=False, include_in_schema=True)):
    p = LapData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update)
    return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
                    media_type="application/json")


@router.post("/get_lapdata")
@logger_decorator
def post_lapdata(request: Request, data: CATANADATA):
    p = LapData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update)
    return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
                    media_type="application/json")


@router.get("/get_otherdata")
@logger_decorator
def get_otherdata(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                  Variables: List[str] = Query(default=[]),
                  RunUID: List[str] = Query(default=[]),
                  Years: List[int] = Query(default=[]),
                  Update: bool = Query(default=False, include_in_schema=True)):
    p = OtherData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update)
    res = {k: parse_df(v) for k, v in result.items()}
    return Response(json.dumps(res), media_type="application/json")


@router.post("/get_otherdata")
@logger_decorator
def post_otherdata(request: Request, data: CATANADATA):
    p = OtherData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update)
    res = {k: parse_df(v) for k, v in result.items()}
    return Response(json.dumps(res), media_type="application/json")


@router.get("/get_rundata")
@logger_decorator
def get_rundata(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                Variables: List[str] = Query(default=[]),
                RunUID: List[str] = Query(default=[]),
                Years: List[int] = Query(default=[]),
                Update: bool = Query(default=False, include_in_schema=True)):
    p = RunData(Competition, Variables, RunUID, Years)
    result = p.process_data(Update)
    return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
                    media_type="application/json")


@router.post("/get_rundata")
@logger_decorator
def post_rundata(request: Request, data: CATANADATA):
    p = RunData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data(data.Update)
    return Response(result.to_json(orient="split", date_format="iso", default_handler=str),
                    media_type="application/json")

def _get_correct_entity(competition: CompetitionEnum,
                        data_type: CatanaDataTypeEnum,
                        run_uid: list[str], years: list[str]):

    entities = {
        CatanaDataTypeEnum.CDCDATA: CDCData,
        CatanaDataTypeEnum.CHANNEL: ChannelData,
        CatanaDataTypeEnum.HISTO: HistoData,
        CatanaDataTypeEnum.HISTO2D: Histo2DData,
        CatanaDataTypeEnum.HISTOLAPDATA: HistoLapData,
        CatanaDataTypeEnum.LAPDATA: LapData,
        CatanaDataTypeEnum.OTHER: OtherData,
        CatanaDataTypeEnum.RUNDATA: RunData
    }
    return entities[data_type](competition, None, run_uid, years)


@router.get("/get_availabledata")
@logger_decorator
def get_avaliabledata(request: Request,
                      Competition: CompetitionEnum = Query(
                          default=CompetitionEnum.PUAS3),
                      DataType: CatanaDataTypeEnum = Query(
                          default=CatanaDataTypeEnum.LAPDATA),
                      RunUID: List[str] = Query(default=[]),
                      Years: List[int] = Query(default=[])):
    p = _get_correct_entity(Competition, DataType, RunUID, Years)
    result = p.list_variables()
    return Response(json.dumps(result), media_type="application/json")


@router.post("/get_availabledata")
@logger_decorator
def post_avaliabledata(request: Request, data: VARIABLES):
    p = _get_correct_entity(
        data.Competition, data.DataType, data.RunUID, data.Years)
    result = p.list_variables()
    return Response(json.dumps(result), media_type="application/json")


@router.get("/get_channels")
@logger_decorator
async def get_channels(request: Request, Competition: CompetitionEnum = Query(default=CompetitionEnum.PUAS3),
                 Variables: List[str] = Query(default=[]),
                 RunUID: List[str] = Query(default=[]),
                 Years: List[int] = Query(default=[])):
    p = ChannelData(Competition, Variables, RunUID, Years)
    result = p.process_data()
    res = {k: parse_df(v) for k,v in result.items()} 
    return StreamingResponse(json_dumps_iterator(res),
                             media_type="application/json")


@router.post("/get_channels")
async def post_channels(request: Request, data: CATANADATA):
    ip = request.client.host
    logger.info(f' {ip} | Starting post_channels\n')
    p = ChannelData(data.Competition, data.Variables, data.RunUID, data.Years)
    result = p.process_data()
    res = {k: parse_df(v) for k,v in result.items()} 
    return StreamingResponse(json_dumps_iterator(res),
                             media_type="application/json")
