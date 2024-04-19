"""
File : /scripts/process_relational.py

Description:
1. Create a MySQL database with desired tables
2. Push data into our MySQL database
"""
# from utils import connMySQL  # Make sure this utility now returns a MySQL connection
import json
from utils import connect_to_mysql
import mysql.connector
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def createMySQLTables(_cursor):
    """
    Creates six tables in a MySQL database using SQL CREATE TABLE statements.

    Parameters:
        _cursor (cursor): A cursor object used to execute SQL commands in MySQL.

    Returns:
        None
    """
    try:
        # User profile table -> table 1
        _cursor.execute(
            """
            CREATE TABLE user_profile
            (
                user_id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(50),
                screen_name VARCHAR(15),
                url TEXT NULL,
                location TEXT NULL,
                followers_count INT NULL,
                friends_count INT,
                listed_count INT,
                favourites_count INT,
                statuses_count INT,
                created_at TIMESTAMP,
                description TEXT,
                language CHAR(4),
                verified BOOLEAN,
                profile_image_url TEXT,
                profile_background_image_url TEXT NULL,
                default_profile BOOLEAN NULL,
                default_profile_image BOOLEAN NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        )
        print('\t Table user_profile Create Successful')
    except Exception as e:
        print(f'\t Table user_profile Create Unsuccessful as {e}')

    try:
        # Reply tweet table -> table 2
        _cursor.execute(
            """
            CREATE TABLE reply
            (
                reply_tweet_id VARCHAR(255) PRIMARY KEY,
                tweet_id VARCHAR(255)
            )
            """
        )
        print('\t Table reply Create Successful')
    except Exception as e:
        print(f'\t Table reply Create Unsuccessful as {e}')

    try:
        # Quoted tweet table -> table 3
        _cursor.execute(
            """
            CREATE TABLE quoted_tweets
            (
                quoted_tweet_id VARCHAR(255) PRIMARY KEY,
                tweet_id VARCHAR(255)
            )
            """
        )
        print('\t Table quoted_tweets Create Successful')
    except Exception as e:
        print(f'\t Table quoted_tweets Create Unsuccessful as {e}')

    try:
        # Retweet table -> table 4
        _cursor.execute(
            """
            CREATE TABLE retweets
            (
                retweet_id VARCHAR(255) PRIMARY KEY,
                tweet_id VARCHAR(255)
            )
            """
        )
        print('\t Table retweets Create Successful')
    except Exception as e:
        print(f'\t Table retweets Create Unsuccessful as {e}')

    try:
        # Tweets table -> table 5
        _cursor.execute(
            """
            CREATE TABLE tweets
            (
                tweet_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255),
                tweet_created_at TIMESTAMP,
                tweet_flag VARCHAR(20)
            )
            """
        )
        print('\t Table tweets Create Successful')
    except Exception as e:
        print(f'\t Table tweets Create Unsuccessful as {e}')
    
    try:
        # Logs table -> table 6
        _cursor.execute(
            """
            CREATE TABLE logs
            (
                query TEXT,
                created_at TIMESTAMP,  # Adjusted for MySQL TIMESTAMP usage
                time_taken DECIMAL(10, 3)  # Precision and scale defined
            )
            """
        )
        print('\t Table logs Create Successful')
    except Exception as e:
        print(f'\t Table logs Create Unsuccessful as {e}')



def convert_twitter_date_to_sql_date(twitter_date):
    try:
        dt = datetime.strptime(twitter_date, '%a %b %d %H:%M:%S +0000 %Y')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Date conversion error: {e}")
        return None

def pushMySQLData(cursor, filename="corona-out-3"):
    seen_users = set()  # To track users we've already added
    seen_tweets = set()  # To track tweets we've already processed

    with open(filename, "r") as f1:
        for line in f1:
            try:
                data = json.loads(line)

                tweet_id = data['id_str']
                if tweet_id in seen_tweets:
                    continue  # Skip this tweet if we have seen it before
                seen_tweets.add(tweet_id)

                # User data
                user = data['user']
                user_id = user['id_str']
                user_created_at = convert_twitter_date_to_sql_date(user['created_at'])
                if user_id not in seen_users and user_created_at:
                    insert_user_query = """
                        INSERT INTO user_profile (user_id, name, screen_name, url, location, followers_count, friends_count, listed_count, favourites_count, statuses_count, created_at, description, language, verified, profile_image_url, profile_background_image_url, default_profile, default_profile_image)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE name=VALUES(name), location=VALUES(location), followers_count=VALUES(followers_count), friends_count=VALUES(friends_count), statuses_count=VALUES(statuses_count), description=VALUES(description);
                    """
                    cursor.execute(insert_user_query, (
                        user_id, user['name'], user['screen_name'], user.get('url'), user.get('location'),
                        user['followers_count'], user['friends_count'], user['listed_count'], user['favourites_count'],
                        user['statuses_count'], user_created_at, user.get('description'), user.get('lang'),
                        user['verified'], user.get('profile_image_url'), user.get('profile_background_image_url'),
                        user['default_profile'], user['default_profile_image']))
                    seen_users.add(user_id)

                # Tweet data
                tweet_created_at = convert_twitter_date_to_sql_date(data['created_at'])
                if tweet_created_at:
                    tweet_flag = 'retweeted' if 'retweeted_status' in data else 'original'
                    insert_tweet_query = """
                        INSERT INTO tweets (tweet_id, user_id, tweet_created_at, tweet_flag)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), tweet_created_at=VALUES(tweet_created_at);
                    """
                    cursor.execute(insert_tweet_query, (tweet_id, user_id, tweet_created_at, tweet_flag))

                # Retweets
                if 'retweeted_status' in data:
                    retweet_id = data['retweeted_status']['id_str']
                    insert_retweet_query = """
                        INSERT INTO retweets (retweet_id, tweet_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE tweet_id=VALUES(tweet_id);
                    """
                    cursor.execute(insert_retweet_query, (retweet_id, tweet_id))

                # Replies
                if data['in_reply_to_status_id_str']:
                    insert_reply_query = """
                        INSERT INTO reply (reply_tweet_id, tweet_id)
                        VALUES (%s, %s)
                    """
                    cursor.execute(insert_reply_query, (data['in_reply_to_status_id_str'], tweet_id))

                # Quoted tweets
                if data.get('is_quote_status', False) and 'quoted_status_id_str' in data:
                    quoted_tweet_id = data['quoted_status_id_str']
                    insert_quoted_query = """
                        INSERT INTO quoted_tweets (quoted_tweet_id, tweet_id)
                        VALUES (%s, %s)
                    """
                    cursor.execute(insert_quoted_query, (quoted_tweet_id, tweet_id))
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON at line {line_number}: {e}")
                continue  # Skip to the next line
            except mysql.connector.Error as err:
                print(f"SQL Error: {err}")
            except Exception as e:
                print(f"General Error: {e}")


# if __name__ == "__main__":
#     connection = connect_to_mysql()  # Direct MySQL connection
#     if connection:
#         try:
#             with connection.cursor() as cursor:
#                 # Create the MySQL tables
#                 print("Starting the table creation process...")
#                 createMySQLTables(cursor)
#                 connection.commit()  # Commit the table creation before inserting data
#                 print("Table creation process completed.")

#                 # Push data into MySQL
#                 print("Starting data insertion process...")
#                 pushMySQLData(cursor)  # You can specify a different filename as an argument if needed
#                 connection.commit()  # Commit the data push to make sure all changes are saved
#                 print("Data insertion process completed.")
        
#         except Exception as e:
#             print(f"An error occurred: {e}")
#             connection.rollback()  # Roll back any changes if there was an error

#         finally:
#             connection.close()  # Close the MySQL connection
#             print("MySQL connection closed.")
#     else:
#         print("Failed to connect to MySQL.")



if __name__ == "__main__":
    connection = connect_to_mysql()
    if connection:
        cursor = None
        try:
            cursor = connection.cursor()
            print("Starting the table creation process...")
            createMySQLTables(cursor)  # Attempt to create tables
            connection.commit()  # Commit the transaction if successful
            print("Table creation process completed.")

            # If there's more processing, include it here
            print("Starting data insertion process...")
            pushMySQLData(cursor)  # Insert data
            connection.commit()  # Commit the data insertion
            print("Data insertion process completed.")

        except Exception as e:
            print(f"An error occurred: {e}")
            if connection.is_connected():
                connection.rollback()  # Roll back any changes if an error occurred
        finally:
            if cursor is not None:
                cursor.close()  # Ensure the cursor is closed after the operation
            if connection.is_connected():
                connection.close()  # Ensure the connection is closed
            print("MySQL connection closed.")
    else:
        print("Failed to connect to MySQL.")


