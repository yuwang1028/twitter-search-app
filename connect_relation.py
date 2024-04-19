import json
from utils import connect_to_mysql
import mysql.connector
import warnings
warnings.filterwarnings('ignore')


def fetch_table_details(connection):
    """ Fetches and prints the number of columns and rows for each table in the database """
    cursor = connection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = database();")
    tables = cursor.fetchall()
    print("Database Tables and their respective sizes:")
    for (table_name,) in tables:
        # Check and decode bytearray to string
        if isinstance(table_name, (bytes, bytearray)):
            table_name = table_name.decode('utf-8')

        # Debug print to check what is passed to the SQL query
        print(f"Querying details for: {table_name}, Type: {type(table_name)}")

        # Use the correctly formatted SQL queries using backticks and string formatting
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            rows_count = cursor.fetchone()[0]
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns_count = len(cursor.fetchall())
            print(f"Table: {table_name}, Columns: {columns_count}, Rows: {rows_count}")
        except mysql.connector.Error as err:
            print(f"Error querying table {table_name}: {err}")

if __name__ == "__main__":
    conn = connect_to_mysql()
    if conn:
        try:
            fetch_table_details(conn)
        finally:
            conn.close()
            print("MySQL connection closed.")
