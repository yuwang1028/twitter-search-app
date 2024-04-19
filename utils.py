import os
import json
from configparser import ConfigParser
import mysql.connector
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

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
