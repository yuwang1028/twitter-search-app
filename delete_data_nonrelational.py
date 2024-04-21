from pymongo import MongoClient
from utils import connect_to_mongodb

def delete_all_data(db_name, collection_name):
    client = connect_to_mongodb()
    if client is None:
        print("MongoDB connection failed.")
        return

    db = client[db_name]
    collection = db[collection_name]

    try:
        delete_result = collection.delete_many({})
        print(f"Total documents deleted: {delete_result.deleted_count}")
    except Exception as e:
        print(f"Error while trying to delete data: {e}")

if __name__ == "__main__":
    db_name = 'twitter'         # The name of the database
    collection_name = 'nonrelational'  # The name of the collection
    delete_all_data(db_name, collection_name)

