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
from mysql.connector.errors import IntegrityError
from datetime import datetime
import signal
import sys
import warnings
warnings.filterwarnings('ignore')


class CustomJSONEncoder(json.JSONEncoder):
    """
    This is a custom class that extends the `json.JSONEncoder` class. It overrides the `default()` method
    to handle datetime objects in a JSON-serializable format.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

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
            CREATE TABLE IF NOT EXISTS user_profile 
            (
                user_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci PRIMARY KEY,
                name VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                screen_name VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                url TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                location TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                followers_count INT NULL,
                friends_count INT,
                listed_count INT,
                favourites_count INT,
                statuses_count INT,
                created_at TIMESTAMP,
                description TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                language CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                verified BOOLEAN,
                profile_image_url TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                profile_background_image_url TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                default_profile BOOLEAN NULL,
                default_profile_image BOOLEAN NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        )
        print('\tTable user_profile Create Successful')
    except Exception as e:
        print(f'\tTable user_profile Create Unsuccessful: {e}')
    

    try:
        # Tweets table -> table 2
        _cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tweets
            (
                tweet_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                tweet_created_at TIMESTAMP,
                tweet_flag VARCHAR(20)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        )
        print('\tTable tweets Create Successful')
    except Exception as e:
        print(f'\tTable tweets Create Unsuccessful: {e}')

    try:
        # Reply tweet table -> table 3
        _cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reply
            (
                reply_tweet_id VARCHAR(255) PRIMARY KEY,
                tweet_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        )
        print('\t Table reply Create Successful')
    except Exception as e:
        print(f'\t Table reply Create Unsuccessful as {e}')

    try:
        # Quoted tweet table -> table 4
        _cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS quoted_tweets
            (
                quoted_tweet_id VARCHAR(255) PRIMARY KEY,
                tweet_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        )
        print('\t Table quoted_tweets Create Successful')
    except Exception as e:
        print(f'\t Table quoted_tweets Create Unsuccessful as {e}')

    try:
        # Retweet table -> table 5
        _cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS retweets
            (
                retweet_id VARCHAR(255) PRIMARY KEY,
                tweet_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        )
        print('\t Table retweets Create Successful')
    except Exception as e:
        print(f'\t Table retweets Create Unsuccessful as {e}')

    try:
        # Logs table -> table 6
        _cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS logs
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


def create_index(cursor, index_name, table_name, column_name):
    try:
        cursor.execute(f"""
            CREATE INDEX {index_name} ON {table_name} ({column_name});
        """)
        print(f"Index {index_name} created successfully on {table_name}.")
    except mysql.connector.Error as err:
        if "Duplicate key name" in str(err):
            print(f"Index {index_name} already exists on {table_name}.")
        else:
            raise


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
        for line_number, line in enumerate(f1, start=1):
            if line_number <= 194108:
                continue
            try:
                data = json.loads(line.strip())

                tweet_id = data['id_str']
                if tweet_id in seen_tweets:
                    continue  # Skip this tweet if we have seen it before
                seen_tweets.add(tweet_id)

                # User data
                user = data['user']
                user_id = user['id_str']
                user_created_at = convert_twitter_date_to_sql_date(user['created_at'])
                if user_id not in seen_users and user_created_at:
                    try:
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
                    except IntegrityError:
                        print(f"Skipping duplicate user entry for ID {user_id}")

                # Tweet data
                tweet_created_at = convert_twitter_date_to_sql_date(data['created_at'])
                if tweet_created_at:
                    try:
                        tweet_flag = 'retweeted' if 'retweeted_status' in data else 'original'
                        insert_tweet_query = """
                            INSERT INTO tweets (tweet_id, user_id, tweet_created_at, tweet_flag)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), tweet_created_at=VALUES(tweet_created_at);
                        """
                        cursor.execute(insert_tweet_query, (tweet_id, user_id, tweet_created_at, tweet_flag))
                    except IntegrityError:
                        print(f"Skipping duplicate tweet for Tweet ID {tweet_id}")

                # Retweets
                if 'retweeted_status' in data:
                    retweet_id = data['retweeted_status']['id_str']
                    try:
                        insert_retweet_query = """
                            INSERT INTO retweets (tweet_id, retweet_id)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE tweet_id=VALUES(tweet_id);
                        """
                        cursor.execute(insert_retweet_query, (retweet_id, tweet_id))
                    except IntegrityError:
                        print(f"Skipping duplicate retweet entry for Tweet ID {tweet_id}")

                # Replies
                if data['in_reply_to_status_id_str']:
                    reply_tweet_id = data['in_reply_to_status_id_str']
                    try:
                        insert_reply_query = """
                            INSERT INTO reply (tweet_id, reply_tweet_id)
                            VALUES (%s, %s)
                        """
                        cursor.execute(insert_reply_query, (tweet_id, reply_tweet_id))
                    except IntegrityError:
                        print(f"Skipping duplicate reply for Tweet ID {tweet_id}")

                # Quoted tweets
                if data.get('is_quote_status', False) and 'quoted_status_id_str' in data:
                    quoted_tweet_id = data['quoted_status_id_str']
                    try:
                        insert_quoted_query = """
                            INSERT INTO quoted_tweets (tweet_id, quoted_tweet_id)
                            VALUES (%s, %s)
                        """
                        cursor.execute(insert_quoted_query, (tweet_id, quoted_tweet_id))
                    except IntegrityError:
                        print(f"Skipping duplicate quoted tweet for Tweet ID {tweet_id}")

            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON at line {line_number}: {e}")
                continue  # Skip to the next line
            except mysql.connector.Error as err:
                print(f"SQL Error: {err}")
                continue  # Continue with next line in case of SQL error
            except Exception as e:
                print(f"General Error at line {line_number}: {e}")


# Define the signal handler function
def signal_handler(signum, frame):
    print("Timed out!")
    sys.exit(1)  # Exit the script with error status 1

# Register the signal handler for the SIGALRM signal
signal.signal(signal.SIGALRM, signal_handler)


# Function to setup and manage database operations
def manage_database_operations():
    connection = connect_to_mysql()
    if connection:
        cursor = None
        try:
            cursor = connection.cursor()
            print("Starting the table creation process...")
            createMySQLTables(cursor)
            print("Creating indexes...")
            create_index(cursor, "idx_tweet_id", "tweets", "tweet_id")
            connection.commit()
            print("Database setup completed.")

            print("Starting data insertion process...")
            pushMySQLData(cursor)
            connection.commit()
            print("Data insertion process completed.")
        except Exception as e:
            print(f"An error occurred: {e}")
            if connection.is_connected():
                connection.rollback()
        finally:
            if cursor:
                cursor.close()
            if connection.is_connected():
                try:
                    connection.commit()  # Attempt to commit any remaining changes
                except Exception as commit_error:
                    print(f"Error during final commit: {commit_error}")
                connection.close()
            print("MySQL connection closed.")
    else:
        print("Failed to connect to MySQL.")

if __name__ == "__main__":
    # Set a timeout of 600 seconds (10 minutes)
    signal.alarm(5400)  # This line sets the alarm

    try:
        manage_database_operations()
    except KeyboardInterrupt:
        print("Script interrupted by user")
    except SystemExit:
        print("Script terminated due to timeout")
    finally:
        signal.alarm(0)  # Disable the alarm

    print("Script execution finished")
