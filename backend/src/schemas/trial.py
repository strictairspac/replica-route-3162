from typing import Literal
from pydantic import BaseModel,Field
class TrialInput(BaseModel):
    name:str=Field(min_length=2,max_length=120)
    request_set_id:int=Field(gt=0)
    max_delay_ms:int=Field(default=100,ge=0,le=3600000)
    sticky_ms:int=Field(default=500,ge=0,le=3600000)
    snapshot_retention:int=Field(default=8,ge=1,le=50)
    scan_serving:Literal['pinned','portable']=Field(default='pinned')
