from typing import Union
from pydantic import BaseModel

class log_input(BaseModel):
    script_name: str
    timestamp: str
    data: Union[str, dict]

class response(BaseModel):
    store: bool
    time: float

