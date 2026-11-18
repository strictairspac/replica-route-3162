from typing import Literal
from pydantic import BaseModel,Field,model_validator
class Event(BaseModel):
    at_ms:int=Field(ge=0,le=86400000)
    kind:Literal['write','read','replicate','node_down','node_up','kv_put','kv_delete','range_page']
    session:str=Field(default='default',min_length=1,max_length=60)
    node:str|None=Field(default=None,max_length=80)
    position:int|None=Field(default=None,ge=0,le=1000000000)
    delay_ms:int|None=Field(default=None,ge=0,le=3600000)
    # 键值与范围分页字段
    key:str|None=Field(default=None,max_length=120)
    value:str|None=Field(default=None,max_length=500)
    scan_id:str|None=Field(default=None,max_length=60)
    page_no:int|None=Field(default=None,ge=1)
    page_size:int|None=Field(default=None,ge=1,le=200)
    start_key:str|None=Field(default=None,max_length=120)
    end_key:str|None=Field(default=None,max_length=120)
    retry:bool=False
    @model_validator(mode='after')
    def valid_event(self):
        if self.kind in ('replicate','node_down','node_up') and not self.node:raise ValueError('节点事件需要node')
        if self.kind=='replicate' and (self.position is None or self.delay_ms is None):raise ValueError('replicate需要position和delay_ms')
        if self.kind in ('kv_put','kv_delete') and not self.key:raise ValueError('键值事件需要key')
        if self.kind=='kv_put' and not self.value:raise ValueError('kv_put需要value')
        if self.kind=='range_page':
            if not self.scan_id or not self.page_no:raise ValueError('range_page需要scan_id和page_no')
            if self.page_no==1 and not self.start_key:raise ValueError('分页首屏需要start_key')
        return self
class RequestSetInput(BaseModel):
    code:str=Field(min_length=2,max_length=80)
    title:str=Field(min_length=2,max_length=160)
    events:list[Event]=Field(min_length=1,max_length=100)
    note:str=Field(default='',max_length=300)
    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms>b.at_ms for a,b in zip(self.events,self.events[1:])):raise ValueError('事件必须按at_ms非递减排列')
        return self
