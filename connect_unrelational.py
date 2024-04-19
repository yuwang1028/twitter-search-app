import warnings
from utils import connect_to_mongodb

warnings.filterwarnings('ignore')

def test_mongodb_connection():
    print("Testing MongoDB connection...")
    client = connect_to_mongodb()
    if client:
        print("Successfully connected to MongoDB.")
        client.close()  # It's important to close the connection after testing
    else:
        print("Failed to connect to MongoDB.")

if __name__ == "__main__":
    test_mongodb_connection()  
