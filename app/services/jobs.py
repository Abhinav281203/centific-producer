import json
from databricks_api import DatabricksAPI
from typing import Set, Dict, List
from ..utils import need


# all_jobs returns a list of jobs associated to the account
# pass the db object and a set of strings called needs, the jobs returned will have the passed 'needs' attributes only
# if all attributes are needed just pass empty set
def all_jobs(db: DatabricksAPI, needs: Set[str] = set()) -> Dict[str, any]:
    try:
        jobs = db.jobs.list_jobs()["jobs"]
        return {"status": True, "data": need.need_only(needs, jobs)}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> list_clusters E : {e}")
        return {"status": False, "data": str(e)}


# will create a job dunno if non ipynbs will work
# job name is a string which is going to be the name of the pipeline
# task_names is a list of strings where each item in list is the name of each task
# paths is a list of notebook paths (absolute)
# dependents a list where each item is a list string, if no dependents pass an empty list to that task
# cluster ids a list of strings where each string is a cluster assigned
"""
 Sample tasks that is sent to the api
 tasks = [
 {
 "task_key": "task_1",
 "notebook_task": {
 "notebook_path": "/Workspace/Users/chhnvngl@gmail.com/code_multicellx.ipynb"
 },
 "existing_cluster_id": "0911-043013-sm1e4k6v"
 },
 {
 "task_key": "task_2",
 "notebook_task": {
 "notebook_path": "/Workspace/Users/chhnvngl@gmail.com/trash222.ipynb"
 },
 "existing_cluster_id": "0911-043013-sm1e4k6v",
 "depends_on": [{"task_key": "task_1"}]
 }
 ]"""


def create_job(
    db: DatabricksAPI,
    job_name: str,
    task_names: List[str],
    paths: List[str],
    dependents: List[List[str]],
    cluster_ids: List[str],
) -> Dict[str, any]:
    if not len(task_names) == len(paths) == len(dependents) == len(cluster_ids):
        print("jobs.py -> create_job() -> E : len of parameters not same")
        return {"status": False, "data": "len of passed parameters are different"}
    try:
        tasks = []
        for i in range(len(task_names)):
            this_task = {
                "task_key": task_names[i],
                "notebook_task": {"notebook_path": paths[i]},
                "existing_cluster_id": cluster_ids[i],
            }
            if len(dependents[i]) > 0:
                this_task["depends_on"] = []
                for parent in dependents[i]:
                    this_task["depends_on"].append({"task_key": parent})
            tasks.append(this_task)
        job_config = {
            "name": job_name,
            "tasks": tasks,
        }
        job_id = db.jobs.create_job(**job_config)
        return {"status": True, "data": job_id}
    except Exception as e:
        print("jobs.py -> create_job() -> E : ", e)
        return {"status": False, "data": str(e)}


# run job takes db object and job_id as input parameters
# triggers the run of the whole job and all tasks within
def run_job(db: DatabricksAPI, job_id: str) -> Dict[str, any]:
    try:
        response = db.jobs.run_now(job_id=job_id)
        return {"status": True, "data": response}
    except Exception as e:
        print("jobs.py -> run_job() -> E : ", e)
        return {"status": False, "data": str(e)}


# returns a job metadata
def get_job(db: DatabricksAPI, job_id: str, needs: Set[str] = set()) -> Dict[str, any]:
    try:
        response = db.jobs.get_job(job_id=job_id)
        return {"status": True, "data": need.need_only(needs, [response])}
    except Exception as e:
        print("jobs.py -> get_job() -> E : ", e)
        return {"status": False, "data": str(e)}


# gets all runs of job
def get_job_runs(
    db: DatabricksAPI, job_id: str, needs: Set[str] = set()
) -> Dict[str, any]:
    try:
        response = db.jobs.list_runs(job_id=job_id)["runs"]
        return {"status": True, "data": need.need_only(needs, response)}
    except Exception as e:
        print("jobs.py -> get_job_runs() -> E : ", e)
        return {"status": False, "data": str(e)}


# gets run infomration
def get_run(db: DatabricksAPI, run_id: str, needs: Set[str] = set()) -> Dict[str, any]:
    try:
        response = db.jobs.get_run(run_id=run_id)
        return {"status": True, "data": need.need_only(needs, [response])}
    except Exception as e:
        print("jobs.py -> get_job_runs() -> E : ", e)
        return {"status": False, "data": str(e)}
