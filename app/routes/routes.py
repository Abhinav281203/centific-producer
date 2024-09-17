from fastapi import APIRouter, HTTPException, Query
from typing import List
import databricks_api
import os
from ..models import models
from ..services import bricks_auth
from ..services import clusters
from ..services import jobs
from ..services import workspace
from ..utils import lyra
from config import development

app = APIRouter()

# Authenticates with databricks
# returns true if auth is successful else false
def authenticate(host = None, token = None):
    if host == None or token == None:
        db_object = bricks_auth.bricks_object(development.DATABRICKS_HOST, 
                                            development.DATABRICKS_TOKEN)
    else:
        db_object = bricks_auth.bricks_object(host=host, 
                                            token=token)
    if bricks_auth.verify(db_object):
        print("Successfully authenticated")
        return {"status": True, "data": db_object}
    else:
        return {'status' : False, "data" : "invalid host or token"}
    

# Gets all the clusters associated with the account
# with optional list of attributes to filter search
@app.get("/clusters/list", tags=["clusters"])
def get_clusters(needs: List = Query([], description="List of attributes to filter on")):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = clusters.list_clusters(data, needs=set(needs))
    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Creates a job with mentioned tasks
# returns true if successful else false
@app.post("/jobs/create", tags=["jobs"])
def create_job(job: models.Job):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    if not len(job.task_names) == len(job.paths) == len(job.dependents) == len(job.cluster_ids):
        raise HTTPException(status_code=400, detail="Bad request, parameters not same length")
    
    result = jobs.create_job(
        db=data,
        job_name=job.job_name,
        task_names=job.task_names,
        paths=job.paths,
        dependents=job.dependents,
        cluster_ids=job.cluster_ids
    )

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Gets all the jobs/pipelines associated with the account
# with optional list of attributes to filter search
@app.get("/jobs/list", tags=["jobs"])
def get_jobs(needs: List = Query([], description="List of attributes to filter on")):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = jobs.all_jobs(data, needs=set(needs))
    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Gets complete metadata of a job
@app.get("/jobs/metadata", tags=["jobs"])
def get_job(job_id: str, needs: List = Query([], description="List of attributes to filter on")):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = jobs.get_job(data, job_id=job_id, needs=set(needs))

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Runs a job with valid jab_id
@app.post("/jobs/run", tags=["jobs"])
def run_job(job_id: str):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    job =  get_job(job_id=job_id, needs=[])
    if not job["status"]:
        raise HTTPException(status_code=400, detail="Bad request, Job with job_id doesn't exist")
    
    result = jobs.run_job(data, job_id=job_id)

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}


# Gets information of a specific run using run_id of a job / tasks in job
@app.get("/jobs/run/info", tags=["jobs"])
def get_job_run_info(run_id: str, needs: List = Query([], description="List of attributes to filter on")):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = jobs.get_run(data, run_id=run_id, needs=set(needs))

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Gets information of all the runs of a job
@app.get("/jobs/run/allruns", tags=["jobs"])
def get_job_runs_info(job_id: str, needs: List = Query([], description="List of attributes to filter on")):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    job = get_job(job_id=job_id, needs=[])
    if not job["status"]:
        raise HTTPException(status_code=400, detail="Bad request, Job with job_id doesn't exist")

    result = jobs.get_job_runs(data, job_id=job_id, needs=set(needs))

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}


# Gets all the directories and files in the workspace recursively
# with optional path prefix to filter paths
@app.get("/workspace/list", tags=["workspace"])
def get_workspace(prefix: str = Query("/", description="Path prefix to filter files")):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = workspace.all_files(data, path=prefix)
    
    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occurred")
    
    return {"status": True, "data": result["data"]}

# When given a local path and a file path in the workspace
# Reads contents of the ipynb and create the same on the workspace
# todo: Just use the uploaded file to create the notebook
@app.post("/workspace/upload", tags=["workspace"])
def upload_file(file: models.Workspace):
    status, data = authenticate().values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")

    if not os.path.exists(file.local_path):
        raise HTTPException(status_code=400, detail="File doesn't exist")

    notebook_as_json = workspace.read_ipynb(local_path=file.local_path)
    result = workspace.create_notebook(data, path=file.upload_path, content=notebook_as_json)

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occurred")
    
    return {"status": True, "data": result["data"]}

@app.post("/message_lyra", tags=["Lyra"])
def message_lyra(request_body : models.LyraLang) -> dict:
    message = request_body.message
    thread_id = request_body.thread_id
    consumer = request_body.consumer
    host = request_body.host
    token = request_body.token
    
    if not consumer:
        status, data = authenticate().values()
    else:
        status, data = authenticate(host=host, token=token).values()
    if not status:
        raise HTTPException(status_code=401, detail="Not Authorized")
    return lyra.messageGPT(db=data, message=message, thread_id=thread_id)
        

@app.get("/get_thread", tags=["Lyra"])
def get_thread():
    return lyra.getaThread()