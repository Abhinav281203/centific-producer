from typing import List, Dict, Set
from databricks_api import DatabricksAPI
from ..utils import need


# list_clusters returns a list of clusters associated to the account
# pass the db object and a set of strings called needs, the clusters returned will have the passed 'needs' attributes only
# if all attributes are needed just pass empty set
def list_clusters(db: DatabricksAPI, needs: Set[str] = set()) -> Dict[str, any]:
    try:
        clusters = db.cluster.list_clusters()["clusters"]
        return {"status": True, "data": need.need_only(needs, clusters)}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> list_clusters E : {e}")
        return {"status": False, "data": str(e)}


# This is just a fn to return all attributes a cluster could have
def cluster_attributes() -> List[str]:
    return [
        "cluster_id",
        "creator_user_name",
        "driver_healthy",
        "cluster_name",
        "spark_version",
        "azure_attributes",
        "node_type_id",
        "driver_node_type_id",
        "spark_env_vars",
        "autotermination_minutes",
        "enable_elastic_disk",
        "disk_spec",
        "cluster_source",
        "single_user_name",
        "enable_local_disk_encryption",
        "instance_source",
        "driver_instance_source",
        "data_security_mode",
        "runtime_engine",
        "effective_spark_version",
        "state",
        "state_message",
        "start_time",
        "terminated_time",
        "last_state_loss_time",
        "last_activity_time",
        "last_restarted_time",
        "autoscale",
        "default_tags",
        "termination_reason",
        "init_scripts_safe_mode",
    ]
