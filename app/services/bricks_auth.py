from databricks_api import DatabricksAPI
from . import clusters


# returns a db object with the specified host and token, so that it can be used for other functions
def bricks_object(host: str, token: str) -> DatabricksAPI:
    return DatabricksAPI(host=host, token=token)


# auth function this just tries to hit the list clusters endpoint
# The choice of doing this over doing simple token validation is that list_clusters will be able to verify both host and token
def verify(db: DatabricksAPI) -> bool:
    return clusters.list_clusters(db, set())["status"]
