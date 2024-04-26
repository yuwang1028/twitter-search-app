import os
import json
from configparser import ConfigParser
import mysql.connector
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
from psycopg2.extras import Json
from psycopg2 import extensions

class CustomJSONEncoder(json.JSONEncoder):
    """
    This is a custom class that extends the `json.JSONEncoder` class. It overrides the `default()` method
    to handle datetime objects in a JSON-serializable format.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def getConfig(filename='config.ini', section='default'):
    """ Read database configuration file and return a dictionary object """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db

def connect_to_mysql():
    """ Connect to MySQL database using configuration from config.ini """
    db_config = getConfig(section='mysql')
    try:
        print('Connecting to the MySQL database...')
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print('Connection established.')
            return conn
        else:
            print('Connection failed.')
            return None
    except Error as e:
        print(f"Database connection failed: {e}")
        return None

def connect_to_mongodb():
    """
    Connect to MongoDB database using configuration from config.ini
    
    Returns:
        MongoClient object that can be used to interact with the database.
    """
    # Use getConfig to load the MongoDB configuration from the 'mongodb' section
    try:
        mongodb_config = getConfig('config.ini', 'mongodb')
        connection_string = mongodb_config['connection_string']

        print('Connecting to the MongoDB Atlas database...')
        client = MongoClient(connection_string)

        # A simple 'ping' command to check the server is available
        if client.admin.command('ping'):
            print('Connection established.')
            return client
        else:
            print('Connection failed.')
            return None
    except ConnectionFailure as e:
        print(f"Database connection failed: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

async def pushLogs(key, result, response_time):
    """
    This function inserts a new row into a PostgreSQL database table called 'logs' using the provided cursor object.
    The row contains the following values:
    - A JSON-serialized representation of the key (if it is a dictionary), or the key value (if it is not)
    - The response time (in seconds)
    - The response as a JSONB column in the database (as psycopg2.extras.Json)

    Parameters:
    - key: the key associated with the response (can be a dictionary or a simple value)
    - result: the response to be logged (any JSON-serializable object)
    - response_time: the response time in seconds (can be None)
    """
    _connection = connect_to_mysql()
    _cursor = _connection.cursor()

    if isinstance(key, dict):
        key = json.dumps(key)
    
    created_at = datetime.now()
    created_at = json.dumps(created_at,cls=CustomJSONEncoder)



    if bool(key):
        key = extensions.adapt(key).getquoted().decode('utf-8')
        key = key.replace('"', '').replace("%", "%%")
    
    sql_query=f"""
            INSERT INTO
                logs (query, created_at, time_taken)
            VALUES
                ({key},{Json(created_at)},{response_time})
        """
    
    print(sql_query)

    # _cursor.execute(
    #     sql_query
    # )

    _connection.commit()
    _cursor.close()
    _connection.close()