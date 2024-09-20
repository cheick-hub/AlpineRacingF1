import json
from typing import List

from fastapi import APIRouter, Response, Query
from pydantic import BaseModel

from bdd.Analytics import ANALYTICS

router = APIRouter(
    prefix="/analytics",
    tags=["Analytics"],
    responses={404: {"description": "Not found"}},
)

p = ANALYTICS()


# res = p.component_get(ComponentType_list=["ES"])
# print(res.to_json(orient="records", date_format="iso"))


@router.get("/")
async def applications_get(
        Name: List[str] = Query(default=[])):
    return Response(p.get_applications(Name).to_json(orient="records", date_format="iso"),
                    media_type="application/json")


@router.post("/count", include_in_schema=False)
async def applications_count(
        identifier: str = Query(),
        title: str = Query(),
        version: str = Query(),
        Computername: str = Query(),
        UserName: str = Query(),
        Misc: str = Query(default=None)):
    return Response(p.log_count(identifier, title, version, Computername, UserName, Misc).to_json(orient="records",
                                                                                                  date_format="iso"),
                    media_type="application/json")


@router.get("/isValidPassword", include_in_schema=False)
async def isValidPassword(
        UserName: str = Query(),
        PasswordHash: str = Query()):
    r = p.isValidPassword(UserName, PasswordHash)
    j = json.dumps(r)

    return Response(j, media_type="application/json")


@router.get("/getUserType", include_in_schema=False)
async def getUserType(
        UserName: str = Query(),
        Application: str = Query()):
    r = p.getUserType(UserName, Application)
    j = json.dumps(r)

    return Response(j, media_type="application/json")
