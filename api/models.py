from pydantic import BaseModel
from typing import Optional

class Query1Request(BaseModel):
    organization: str
    service: str
    end_date: str
    start_date: str

class Query2Request(BaseModel):
    organization: str
    top_n: int
    end_date: str

class Query3Request(BaseModel):
    end_date: str

class Query4Request(BaseModel):
    year: int
    month: int

class Query5Request(BaseModel):
    start_date: str
    end_date: str
