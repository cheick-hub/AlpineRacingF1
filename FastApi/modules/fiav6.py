from fastapi import APIRouter, Response, Query
from pydantic import BaseModel

from bdd.FIAV6 import FIAV6

router = APIRouter(
    prefix="/fiav6",
    tags=["fiav6"],
    responses={404: {"description": "Not found"}},
)

#
FIAV6()


class FIAMODEL(BaseModel):
    ssn_list: list[str] | None = None


@router.get("/fia_monitoring")
async def fia_monitoring(ssn_list: list[str] = Query(default=None)):
    p = FIAV6()
    result = p.fia_get(ssn_list)
    r = result.to_json(orient="split")
    return Response(r, media_type="application/json")


@router.post("/fia_monitoring")
async def fia_monitoring_post(fiamodel: FIAMODEL):
    p = FIAV6()
    return Response(p.fia_get(fiamodel.__dict__.get("ssn_list")).to_json(orient="split"), media_type="application/json")
