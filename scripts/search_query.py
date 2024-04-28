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

def fetch_searched_tweet_metadata_user_data(SQL_client, username, userscreenname, userverification, start_datetime, end_datetime, filtered_tweet_ids, filtered_user_ids):
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
        # Properly format tweet_ids as string literals for the SQL query
        tweet_ids = ','.join(f"'{id}'" for id in filtered_tweet_ids)  # Note the single quotes around {id}
        query += f" AND t.tweet_id IN ({tweet_ids})"
    if filtered_user_ids:
        user_ids = ','.join(f"'{id}'" for id in filtered_user_ids)  # Note the single quotes around {id}
        query += f" AND t.user_id IN ({user_ids})"


    # Execute the query
    searched_tweet_metadata_user_data = pd.read_sql_query(query, con=SQL_client)
    return searched_tweet_metadata_user_data

def fetch_searched_tweets_data(NoSQL_client, tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime, filtered_tweet_ids):
    # Constructing MongoDB query
    query = {}

    # if start_datetime and end_datetime:
    #     date_format = '%m/%d/%y %I:%M %p'  # Using example format, adjust as necessary
    #     start_datetime = datetime.strptime(start_datetime, date_format)
    #     end_datetime = datetime.strptime(end_datetime, date_format)
    #     query["created_at"] = {"$gte": start_datetime, "$lte": end_datetime}
   
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
    return searched_tweets_data

from scripts.cache import CacheManager
import pandas as pd
import asyncio

def fetch_results(username, userscreenname, userverification, tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime):
    cache_manager = CacheManager()
    search_query = {
        "username": username,
        "userscreenname": userscreenname,
        "userverification": userverification,
        "tweetstring": tweetstring,
        "hashtags": hashtags,
        "tweetsensitivity": tweetsensitivity,
        "tweetcontenttype": tweetcontenttype,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime
    }

    if search_query in cache_manager:
        # If the search query is in cache, retrieve cached data
        cached_data = cache_manager.getQuery(search_query)
        results_df = pd.DataFrame(cached_data)
    else:
        # If the search query is not in cache, perform database queries
        SQL_client = connect_to_mysql()
        NoSQL_client = connect_to_mongodb()
        # Assuming start_datetime and end_datetime are already formatted as required for SQL query.
        filtered_tweet_ids = []  # Assuming this list will be populated based on some condition
        filtered_user_ids = []   # Assuming this list will be populated based on some condition

        # Perform the SQL and NoSQL queries
        tweet_metadata_user_data = fetch_searched_tweet_metadata_user_data(SQL_client, username, userscreenname, userverification, start_datetime, end_datetime, filtered_tweet_ids, filtered_user_ids)
        tweets_data = fetch_searched_tweets_data(NoSQL_client, tweetstring, hashtags, tweetsensitivity, tweetcontenttype, start_datetime, end_datetime, filtered_tweet_ids)

        # Combine the results into a single DataFrame
        if not tweet_metadata_user_data.empty and not tweets_data.empty:
            results_df = pd.merge(tweets_data, tweet_metadata_user_data, how='inner', left_on='id_str', right_on='tweet_id')
        elif not tweet_metadata_user_data.empty:
            results_df = tweet_metadata_user_data
        elif not tweets_data.empty:
            results_df = tweets_data
        else:
            results_df = pd.DataFrame(['No results found'], columns=['Message'])

        # After getting the results, put them in the cache
        if not results_df.empty:
            cache_manager.putQuery(search_query, results_df.to_dict('records'))
            asyncio.run(cache_manager.saveCache())

    return results_df
