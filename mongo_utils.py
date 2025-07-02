import pymongo
import os
from typing import Dict, List, Optional, Any
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.environ.get("MONGO_URI")
DEFAULT_DATABASE = os.environ.get("MONGO_CACHE_DB")


def create_one(collection_name: str, document: Dict) -> str:
    """
    Insert a single document
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.insert_one(document)
    document_id = str(result.inserted_id)

    client.close()
    return document_id


def create_many(collection_name: str, documents: List[Dict]) -> List[str]:
    """
    Insert multiple documents
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.insert_many(documents)
    document_ids = [str(id) for id in result.inserted_ids]

    client.close()
    return document_ids


def read_one(
    collection_name: str, filter_dict: Dict, projection: Optional[Dict] = None
) -> Optional[Dict]:
    """
    Find a single document by filter
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    document = collection.find_one(filter_dict, projection)

    client.close()
    return document


def read_many(
    collection_name: str,
    filter_dict: Optional[Dict] = None,
    projection: Optional[Dict] = None,
    limit: Optional[int] = None,
    sort: Optional[Dict] = None,
) -> List[Dict]:
    """
    Find multiple documents by filter
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    filter_dict = filter_dict or {}
    cursor = collection.find(filter_dict, projection)
    if limit:
        cursor = cursor.limit(limit)

    if sort:
        cursor = cursor.sort(sort)

    documents = cursor.to_list()

    client.close()
    return documents


def read_by_id(collection_name: str, object_id: str) -> Optional[Dict]:
    """
    Find document by ObjectId
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    document = collection.find_one({"_id": ObjectId(object_id)})

    client.close()
    return document


def update_one(
    collection_name: str, filter_dict: Dict, update_dict: Dict, upsert: bool = False
) -> bool:
    """
    Update a single document
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.update_one(filter_dict, {"$set": update_dict}, upsert=upsert)
    updated = result.modified_count > 0

    client.close()
    return updated


def update_many(collection_name: str, filter_dict: Dict, update_dict: Dict) -> int:
    """
    Update multiple documents
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.update_many(filter_dict, {"$set": update_dict})
    modified_count = result.modified_count

    client.close()
    return modified_count


def update_by_id(collection_name: str, object_id: str, update_dict: Dict) -> bool:
    """
    Update document by ObjectId
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.update_one({"_id": ObjectId(object_id)}, {"$set": update_dict})
    updated = result.modified_count > 0

    client.close()
    return updated


def delete_one(collection_name: str, filter_dict: Dict) -> bool:
    """
    Delete a single document
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.delete_one(filter_dict)
    deleted = result.deleted_count > 0

    client.close()
    return deleted


def delete_many(collection_name: str, filter_dict: Dict) -> int:
    """
    Delete multiple documents
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.delete_many(filter_dict)
    deleted_count = result.deleted_count

    client.close()
    return deleted_count


def delete_by_id(collection_name: str, object_id: str) -> bool:
    """
    Delete document by ObjectId
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    result = collection.delete_one({"_id": ObjectId(object_id)})
    deleted = result.deleted_count > 0

    client.close()
    return deleted


def count_documents(collection_name: str, filter_dict: Optional[Dict] = None) -> int:
    """
    Count documents matching filter
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    filter_dict = filter_dict or {}
    count = collection.count_documents(filter_dict)

    client.close()
    return count


def aggregate_documents(collection_name: str, pipeline: List[Dict]) -> List[Dict]:
    """
    Run aggregation pipeline
    """
    mongo_uri = MONGO_URI
    client = pymongo.MongoClient(mongo_uri)
    db = client[DEFAULT_DATABASE]
    collection = db[collection_name]

    documents = collection.aggregate(pipeline).to_list()

    client.close()
    return documents
