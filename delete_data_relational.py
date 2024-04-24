import mysql.connector
from mysql.connector import Error
from utils import connect_to_mysql

def delete_all_data(table_name):
    connection = connect_to_mysql()
    if connection is None:
        print("MySQL connection failed.")
        return

    try:
        cursor = connection.cursor()
        delete_query = f"DELETE FROM {table_name}"
        cursor.execute(delete_query)
        connection.commit()  # Make sure to commit to apply the changes
        print(f"Total rows deleted: {cursor.rowcount}")
    except Error as e:
        print(f"Error while trying to delete data: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    # table_name = 'user_profile'  # Specify the name of the table from which to delete data
    # table_name = 'tweets'
    # table_name = 'reply'
    # table_name = 'quoted_tweets'
    table_name = 'retweets'
    delete_all_data(table_name)