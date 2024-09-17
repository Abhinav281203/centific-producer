import pydantic
from typing import List


class User(pydantic.BaseModel):
    host: str
    token: str


class Job(pydantic.BaseModel):
    job_name: str
    task_names: List[str]
    paths: List[str]
    dependents: List[List[str]]
    cluster_ids: List[str]


class Workspace(pydantic.BaseModel):
    local_path: str
    upload_path: str

class LyraLang(pydantic.BaseModel):
    thread_id : str
    message : str
    host : str
    token : str
    consumer : bool = False