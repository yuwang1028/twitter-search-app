import warnings
from utils import connect_to_mongodb

warnings.filterwarnings('ignore')

def test_mongodb_connection():
    print("Testing MongoDB connection...")
    client = connect_to_mongodb()
    if client:
        print("Successfully connected to MongoDB.")
        return client
    else:
        print("Failed to connect to MongoDB.")
        return None

if __name__ == "__main__":
    client = test_mongodb_connection()
    if client:
        try:
            # Access the database and collection
            db = client['twitter']
            collection = db['nonrelational']
            # Fetch the first 2 documents from the collection
            documents = collection.find().limit(2)
            print("First few documents from the collection 'nonrelational':")
            for doc in documents:
                print(doc)
        except Exception as e:
            print("Error fetching documents: ", e)
        finally:
            client.close()  # It's important to close the connection after testing
    else:
        print("Unable to perform database operations due to failed connection.")
