from utils import connect_to_mongodb  # Assuming connMongoDB is your MongoDB connection function
import json
import time 
import pymongo

def process_document(doc):
    # Select and potentially transform the necessary fields
    processed_doc = {
        'contributors': doc.get('contributors'),
        'text': doc.get('text'),
        'source': doc.get('source'),
        'id_str': str(doc.get('id_str')),
        'created_at': doc.get('created_at'),
        'user_id': doc['user']['id_str'] if 'user' in doc and 'id_str' in doc['user'] else None,
        'truncated': doc.get('truncated'),
        'lang': doc.get('lang'),
        'quote_count': doc.get('quote_count'),
        'reply_count': doc.get('reply_count'),
        'retweet_count': doc.get('retweet_count'),
        'favorite_count': doc.get('favorite_count'),
        'favorited': doc.get('favorited'),
        'retweeted': doc.get('retweeted'),
        'possibly_sensitive': doc.get('possibly_sensitive'),
        'withheld_in_countries': doc.get('withheld_in_countries'),
        'place': str(doc.get('place')),
        'entities': json.dumps(doc.get('entities')),  # Convert dict to string if necessary
        'extended_entities': json.dumps(doc.get('extended_entities')),
        'quoted_status': json.dumps(doc.get('quoted_status')),
        'retweeted_status': json.dumps(doc.get('retweeted_status'))
    }
    return processed_doc

def setup_collection(db, collection_name):
    collection = db[collection_name]
    # Create a unique index on the 'id_str' field if it doesn't already exist
    collection.create_index([('id_str', 1)], unique=True)
    return collection


def insert_data_from_file(file_path, db_name, collection_name, timeout_seconds=3600):
    client = connect_to_mongodb()
    db = client[db_name]
    collection = setup_collection(db, collection_name)  # Setup collection with unique index
    
    start_time = time.time()
    doc_count = 0
    upserts = 0
    
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            if time.time() - start_time > timeout_seconds:
                print("Reached timeout threshold.")
                break
            try:
                if not line.strip():  # Skip empty lines
                    continue
                document = json.loads(line)
                processed_document = process_document(document)
                result = collection.update_one({'id_str': processed_document['id_str']}, {'$set': processed_document}, upsert=True)
                if result.upserted_id is not None:
                    upserts += 1
                doc_count += 1
                if doc_count % 1000 == 0:
                    print(f'\t Total Documents Inserted or Updated: {doc_count}')
            except json.JSONDecodeError as e:
                print(f'Error parsing JSON at line {line_number}: {e}')
                print(f'Offending line content: "{line.strip()}"')
            except Exception as e:
                print(f'\t Document Insert Unsuccessful for doc_count: {doc_count} as {e}')

    if upserts:
        print(f"Documents inserted or updated: {upserts}.")

if __name__ == "__main__":
    file_path = 'corona-out-3'
    db_name = 'twitter'
    collection_name = 'nonrelational'
    timeout_seconds = 60

    print('MongoDB: *** Data Insertion Started ***')
    insert_data_from_file(file_path, db_name, collection_name, timeout_seconds)
    print('MongoDB: *** Data Insertion Completed or Stopped After Timeout ***')
