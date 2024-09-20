from fastapi import APIRouter, Response, responses, Query
from pydantic import BaseModel

import json

from bdd.CATANA_INSERT import CATANAINSERT
from cache.CacheLiveData import CacheLiveData
from push.Queue import PushQueue
from parquet.WriteLapData import WriteLapData

router = APIRouter(
    prefix="/live",
    tags=["live"],
    responses={404: {"description": "Not found"}},
)

CATANAINSERT()
queue = PushQueue(50)


class PushRunData(BaseModel):
    run_meta: dict

class PushLapData(BaseModel):
    lap_meta: dict

class CacheData(BaseModel):
    priority: int
    to_add: str   # json.loads({RunUID, Years, LapNb, Variables : {name:value}})
    competition: str = 'F1'


@router.get('/')
async def get_async_test(priority: int = Query(default=5),
                         var: str = Query(default='une var'),
                         var2: str = Query(default='une seconde var')):
    c = CATANAINSERT()
    queue.add_task(priority, lambda: c.to_test(var, var2))
    return Response(content=f'Ok return {var} and {var2}', status_code=200)

@router.get('/no_queue')
async def get_async_test(priority: int = Query(default=None),
                         var: str = Query(default='une var'),
                         var2: str = Query(default='une seconde var')):
    c = CATANAINSERT()
    c.to_test(var, var2)
    return Response(content=f'Ok return {var} and {var2}', status_code=200)

@router.post('/insert_new_run')
def post_run_meta(data: PushRunData):
    """
        Not using the PriorityQueue as we need the execution as soon as the request is reveived.
        Even if a task is ongoing with the priority queue, this endpoint will be executed as soon 
        as the request is reveived. Returns a dict : {RunFileName : [id, RunUID]}
    """
    run_meta = data.run_meta

    mandatory_fields = ['RunFileName', 'Complete', 'Source', 'Competition',
                        'FilePath', 'Track', 'EngineType', 'DateofRecording',
                        'StartTime', 'EndTime', 'LastModified']
    not_in = [field for field in mandatory_fields if field not in run_meta]
    if not_in:
        print(f'Missing mandatory key(s): {not_in}')
        return Response(f'Missing mandatory key(s): {not_in}',
                        status_code = 422)

    c = CATANAINSERT()
    res = c.insert_new_run(run_meta)
    return Response(res)

@router.post('/insert_new_lap')
def post_lap_meta(data: PushLapData):
    lap_meta = data.lap_meta
    
    mandatory_fields = ['RunUID', 'LapType', 'FastestLap', 'StartTime',
                        'EndTime', 'LapNumber']
    not_in = [field for field in mandatory_fields if field not in lap_meta]
    if not_in:
        print(f'Missing mandatory key(s): {not_in}')
        return Response(f'Missing mandatory key(s): {not_in}',
                        status_code = 422)
    
    c = CATANAINSERT()
    lap_uid = c.insert_LAPINFO(data.lap_meta)
    return Response(content=lap_uid)

@router.post('/insert_lapdata')
def post_insert_lapdata(data: CacheData):
    lapdata = json.loads(data.to_add)

    mandatory_fields = ['RunUID', 'Variables', 'LapNumber']
    not_in = [field for field in mandatory_fields if field not in lapdata]
    if not_in:
        print(f'Missing mandatory key(s): {not_in}')
        return Response(f'Missing mandatory key(s): {not_in}',
                        status_code = 422)
    
    c = CacheLiveData()
    # queue.add_task(data.priority, lambda: c.insert_new_lapdata(json.loads(data.to_add)))
    inserted = c.insert_new_lapdata(lapdata)
    return responses.JSONResponse(content={'redis_return' : inserted})
    # return Response(content=f'Return request insert Redis',
    #                 status_code=200)

@router.post('/write_parquet')
def get_write_pq(data: CacheData):
    json_data = json.loads(data.to_add)

    mandatory_fields = ['RunUID', 'Variables', 'LapNumber', 'Year']
    not_in = [field for field in mandatory_fields if field not in json_data]
    if not_in:
        print(f'Missing mandatory key(s): {not_in}')
        return Response(f'Missing mandatory key(s): {not_in}',
                        status_code = 422)

    to_add = json_data['Variables']
    if not to_add:
        return Response(content='No data to write', status_code=199)
    vars = list(to_add.keys())
    run_uid = json_data['RunUID']
    years = json_data['Year']
    lap_nb = json_data['LapNumber']
    write_lapdata = WriteLapData(data.competition,
                                 vars, run_uid, years, to_add, lap_nb)
    write_lapdata.write_parquet()

    # queue.add_task(data.priority, lambda: write_lapdata.write_parquet())
    return Response(content=f'Return request write parquet',
                    status_code=200)