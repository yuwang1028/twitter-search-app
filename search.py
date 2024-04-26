import sys
import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

# Ensure that the root folder is correctly added to the path for imports
root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

# Importing database connection functions
from utils import connect_to_mysql, connect_to_mongodb

def fetch_searched_tweet_metadata_user_data(SQL_client, start_datetime, end_datetime, username=None, userscreenname=None, userverification=None, filtered_tweet_ids=None):
    # Format datetime for MySQL
    start_datetime = datetime.strptime(start_datetime, '%m/%d/%y %I:%M %p').strftime('%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_datetime, '%m/%d/%y %I:%M %p').strftime('%Y-%m-%d %H:%M:%S')

    # SQL query for MySQL, correctly handling the single quotes
    query = f"""SELECT t.*, u.name, u.screen_name, u.verified, 
                (SELECT COUNT(*) FROM retweets r WHERE r.tweet_id = t.tweet_id) AS retweet_count,
                (SELECT COUNT(*) FROM quoted_tweets q WHERE q.tweet_id = t.tweet_id) AS quoted_count,
                (SELECT COUNT(*) FROM reply p WHERE p.tweet_id = t.tweet_id) AS reply_count
                FROM tweets t
                JOIN user_profile u ON t.user_id = u.user_id
                WHERE (t.tweet_flag='original' OR t.tweet_flag='quoted' OR t.tweet_flag='retweeted')
                AND t.tweet_created_at BETWEEN '{start_datetime}' AND '{end_datetime}'"""

    if username:
        query += f" AND LOWER(u.name) LIKE LOWER('%{username}%')"
    if userscreenname:
        # Correctly handle the escaping of quotes
        safe_userscreenname = userscreenname.replace("'", "''")
        query += f" AND LOWER(u.screen_name) LIKE LOWER('%{safe_userscreenname}%')"
    if userverification == '2':
        query += " AND u.verified = TRUE"
    elif userverification == '3':
        query += " AND u.verified = FALSE"
    if filtered_tweet_ids:
        tweet_ids = ','.join([str(id) for id in filtered_tweet_ids])
        query += f" AND t.tweet_id IN ({tweet_ids})"

    # Execute the query
    searched_tweet_metadata_user_data = pd.read_sql_query(query, con=SQL_client)
    print('######## META DATA columns ########')
    print(searched_tweet_metadata_user_data.columns)
    print('###################################')
    print('######## META DATA ########')
    print(searched_tweet_metadata_user_data)
    print('###########################')
    return searched_tweet_metadata_user_data

def fetch_searched_tweets_data(NoSQL_client, tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime, filtered_tweet_ids):
    # Handling time conversion for MongoDB's BSON format
    # start_datetime = datetime.strptime('Sat Apr 25 12:21:58 +0000 2020', '%b %d %Y %H:%M:%S')
    # end_datetime = datetime.strptime('Sat Apr 25 12:21:58 +0000 2020', '%b %d %Y %H:%M:%S')
    # print(start_datetime, end_datetime)

    # Constructing MongoDB query
    query = {}
    # query = {
    #     "created_at": "Sat Apr 25 12:21:41 +0000 2020"
    # }    
    # query = {
    #     "created_at": {"$gte": start_datetime, "$lte": end_datetime}
    # }
    if tweetstring:
        query["text"] = {"$regex": tweetstring, "$options": "i"}  # Case-insensitive text search
    if hashtags:
        query["hashtags"] = {"$in": [tag.strip().lower() for tag in hashtags.split(',')]}
    if tweetsensitivity:
        if tweetsensitivity == "2":
            query["possibly_sensitive"] = True
        elif tweetsensitivity == "3":
            query["possibly_sensitive"] = {"$ne": True}
    if tweetcontenttype == "2":
        query["media.type"] = {"$exists": True}

    # Adding filter by tweet_ids if necessary
    if filtered_tweet_ids:
        query["id_str"] = {"$in": filtered_tweet_ids}

    # Fetch data
    # print('---->', query)
    # print('--------->', NoSQL_client['twitter']['nonrelational'])
    tweet_data = NoSQL_client['twitter']['nonrelational'].find(query)
    searched_tweets_data = pd.DataFrame(list(tweet_data))
    print('######## MONGODB columns ########')
    print(searched_tweets_data.columns)
    print('#################################')
    print('######## MONGODB DATA ########')
    print(searched_tweets_data)
    print('##############################')
    return searched_tweets_data

def fetch_results(tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime, username=None, userscreenname=None, userverification=None):
    SQL_client = connect_to_mysql()
    NoSQL_client = connect_to_mongodb()
    filtered_tweet_ids = []

    # Search flow: Relational -> Non-relational
    if username or userscreenname:
        searched_tweet_metadata_user_data = fetch_searched_tweet_metadata_user_data(SQL_client, start_datetime, end_datetime, username, userscreenname, userverification, filtered_tweet_ids)
        # print('------->', searched_tweet_metadata_user_data)
        if not searched_tweet_metadata_user_data.empty:
            filtered_tweet_ids = searched_tweet_metadata_user_data['tweet_id'].tolist()
        # print('------->', filtered_tweet_ids)
        searched_tweets_data = fetch_searched_tweets_data(NoSQL_client, tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime, filtered_tweet_ids)
        # print('---->', searched_tweets_data)
    # Search flow: Non-relational -> Relational
    else:
        searched_tweets_data = fetch_searched_tweets_data(NoSQL_client, tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime, filtered_tweet_ids)
        if not searched_tweets_data.empty:
            filtered_tweet_ids = searched_tweets_data['_id'].tolist()
        searched_tweet_metadata_user_data = fetch_searched_tweet_metadata_user_data(SQL_client, start_datetime, end_datetime, username, userscreenname, userverification, filtered_tweet_ids)

    # Merge and return results if any
    if not searched_tweet_metadata_user_data.empty and not searched_tweets_data.empty:
        results_df = pd.merge(searched_tweets_data, searched_tweet_metadata_user_data, left_on='id_str', right_on='tweet_id', how='inner')
        results_df = results_df.sort_values(by=['favorite_count', 'retweet_count_x'], ascending=False) # Sorting as per favorite_count and retweets_count
        return results_df
    else:
        return pd.DataFrame(['No results found'], columns=['Message'])

def main():
    # Example usage:
    results = fetch_results("", "", "", "", '04/24/01 08:00 AM', '04/26/22 08:00 PM', "", "sivaetb", "")
    print('######## RESULTS columns ########')
    print(results.columns)
    print('#################################')
    print('######## RESULTS ########')
    print(results)
    print('#########################')

if __name__ == "__main__":
    main()
