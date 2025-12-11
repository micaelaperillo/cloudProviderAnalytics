from pydantic import BaseModel
from typing import Optional

class Query1Request(BaseModel):
    organization: str
    service: str
    start_date: str
    end_date: str

class Query2Request(BaseModel):
    organization: str
    top_n: int

class Query3Request(BaseModel):
    organization: str

class Query4Request(BaseModel):
    organization: str
    year: int
    month: int

class Query5Request(BaseModel):
    organization: str
    start_date: str
    end_date: str
