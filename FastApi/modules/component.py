from typing import List

from fastapi import APIRouter, Response, Query

from bdd.PUAS import PUAS3

router = APIRouter(
    prefix="/component",
    tags=["component"],
    responses={404: {"description": "Not found"}},
)

p = PUAS3()


# res = p.component_get()
# res = p.component_get(ComponentType_list=["ES"])
# print(res.to_json(orient="records", date_format="iso"))
#

@router.get("/")
async def component_get(
        ComponentType: List[str] = Query(default=[]),
        ComponentReference: List[str] = Query(default=[]),
        Run: List[str] = Query(default=[]),
        PU: List[str] = Query(default=[])):
    return Response(
        p.component_get(ComponentType, ComponentReference, Run, PU).to_json(orient="records", date_format="iso"),
        media_type="application/json")
