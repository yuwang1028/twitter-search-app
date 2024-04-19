import os
import json
from configparser import ConfigParser
import mysql.connector
import pymongo
from pymongo import MongoClient

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
    """ Creates a connection to a MongoDB database using the configuration values read from a 'config.ini' file. """
    config = getConfig(section='mongodb')
    try:
        print('Connecting to the MongoDB database...')
        client = MongoClient(
            host=config['host'],
            port=int(config['port']),
            username=config['user'],
            password=config['password'],
            authSource=config['authSource']  # usually 'admin'
        )
        # Verifying the MongoDB connection
        print('Connection established to MongoDB.')
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")



