from typing import List

from fastapi import APIRouter, Response, Query

from bdd.PUAS import PUAS3

router = APIRouter(
    prefix="/pu",
    tags=["pu"],
    responses={404: {"description": "Not found"}},
)

p = PUAS3()


@router.get("/")
async def pu_get(
        EngineType: List[str] = Query(default=[]),
        NPUReference: List[str] = Query(default=[]),
        Chassis: List[str] = Query(default=[]),
        ChassisNumber: List[str] = Query(default=[]),
        Driver: List[str] = Query(default=[]),
        Event: List[str] = Query(default=[]),
        Session: List[str] = Query(default=[]),
        RunType: List[str] = Query(default=[]),
        RunNumber: List[int] = Query(default=[]),
        FileName: List[str] = Query(default=[]),
        Component: List[str] = Query(default=[])):
    return Response(p.pu_get(EngineType, NPUReference, Chassis, ChassisNumber, Driver, Event,
                             Session, RunType, RunNumber, FileName, Component).to_json(orient="records"),
                    media_type="application/json")


@router.get("/EngineType")
async def get_enginetype():
    return Response(p.get_enginetype().to_json(orient="values"),
                    media_type="application/json")


@router.get("/Element")
async def get_element():
    return Response(p.get_element().to_json(orient="values"),
                    media_type="application/json")


@router.get("/Reference")
async def get_reference(
        EngineType: List[str] = Query(default=[]),
        Element: str = Query(default=[])):
    return Response(p.get_reference(EngineType, Element).to_json(orient="values"),
                    media_type="application/json")
