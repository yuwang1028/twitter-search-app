import json
from utils import connect_to_mysql
import mysql.connector
import warnings
warnings.filterwarnings('ignore')

def test_connection():
    print("Testing MySQL connection...")
    connection = connect_to_mysql()

if __name__ == "__main__":
    test_connection()
    test_connection()  # To see if a repeated call in the same session causes the issue
