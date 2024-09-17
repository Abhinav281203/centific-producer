from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import certifi
from typing import Dict, Any, List

client_url = "mongodb+srv://dev:potentia@certificate.srcge0k.mongodb.net/?retryWrites=true&w=majority&appName=certificate"


def insert_document(
    database_name: str, collection_name: str, document: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        client = MongoClient(client_url, tlsCAFile=certifi.where())
        db = client[database_name]
        collection = db[collection_name]
        id = collection.insert_one(document).inserted_id
        client.close()
        return {"status": True, "data": str(id)}
    except Exception as e:
        print("mongo module threw except", e)
        return {"status": False, "data": str(e)}


def fetch_documents(
    database_name: str, collection_name: str, query: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        client = MongoClient(client_url, tlsCAFile=certifi.where())
        db = client[database_name]
        collection = db[collection_name]
        documents = list(collection.find(query))
        client.close()
        for doc in documents:
            doc["_id"] = str(doc["_id"])
        return {"status": True, "data": documents}
    except Exception as e:
        return {"status": False, "data": str(e)}
