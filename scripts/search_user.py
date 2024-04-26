import sys
import os
import pandas as pd
import numpy as np

root_folder = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_folder)

from utils import connect_to_mysql, connect_to_mongodb

def fetch_searched_tweet_metadata_user_data(SQL_client, user_id):
    query = f'''
    SELECT t.*, u.name, u.screen_name, u.verified, 
        (SELECT COUNT(*) FROM retweets r WHERE r.tweet_id = t.tweet_id) AS retweet_count,
        (SELECT COUNT(*) FROM quoted_tweets q WHERE q.tweet_id = t.tweet_id) AS quoted_count,
        (SELECT COUNT(*) FROM reply p WHERE p.tweet_id = t.tweet_id) AS reply_count
    FROM tweets t
    JOIN user_profile u ON t.user_id = u.user_id
    WHERE u.user_id = '{user_id}'
    '''
    print(query)
    relational_data_df = pd.read_sql_query(query, con=SQL_client)
    return relational_data_df


def fetch_searched_tweets_data(NoSQL_client, filtered_tweet_ids):
    db_name = 'twitter'
    collection_name = 'nonrelational'
    query = {"id_str": {"$in": filtered_tweet_ids}}  # Assuming IDs are stored as strings in MongoDB
    projection = {
        "_id": 0, "id_str": 1, "text": 1, "entities.hashtags.text": 1,
        "possibly_sensitive": 1, "entities.media.type": 1, "entities.media.url": 1
    }
    cursor = NoSQL_client[db_name][collection_name].find(query, projection)
    searched_tweets_data = pd.DataFrame(list(cursor))

    if searched_tweets_data.empty:
        return searched_tweets_data

    # Post-process the data as before
    searched_tweets_data['hashtags'] = searched_tweets_data.apply(
        lambda x: [tag['text'] for tag in x.get('entities', {}).get('hashtags', [])], axis=1)
    searched_tweets_data['media_type'] = searched_tweets_data.apply(
        lambda x: x.get('entities', {}).get('media', [{}])[0].get('type', np.nan), axis=1)
    searched_tweets_data['media_url'] = searched_tweets_data.apply(
        lambda x: x.get('entities', {}).get('media', [{}])[0].get('url', np.nan), axis=1)

    # Rename and select columns
    desired_columns = ['id_str', 'text', 'hashtags', 'possibly_sensitive', 'media_type', 'media_url']
    searched_tweets_data = searched_tweets_data[desired_columns]
    searched_tweets_data = searched_tweets_data.rename(columns={'id_str': 'tweet_id'})

    return searched_tweets_data

def fetch_user_results(user_id):
    SQL_client = connect_to_mysql()
    NoSQL_client = connect_to_mongodb()
    searched_tweet_metadata_user_data = fetch_searched_tweet_metadata_user_data(SQL_client, user_id)
    set1 = set(searched_tweet_metadata_user_data['tweet_id'].unique())
    print(len(set1))

    if searched_tweet_metadata_user_data.empty:
        empty_df = pd.DataFrame()
        return (empty_df, empty_df, empty_df, empty_df)

    filtered_tweet_ids = searched_tweet_metadata_user_data['tweet_id'].tolist()
    # print(filtered_tweet_ids)
    searched_tweets_data = fetch_searched_tweets_data(NoSQL_client, filtered_tweet_ids)
    set2 = set(searched_tweets_data['tweet_id'].unique())
    print(len(set2))
    print(len(set1.intersection(set2)))
    print(len(set2.intersection(set1)))

    if searched_tweets_data.empty:
        empty_df = pd.DataFrame()
        return (empty_df, empty_df, empty_df, empty_df)

    results_df = pd.merge(searched_tweets_data, searched_tweet_metadata_user_data, on='tweet_id', how='inner')
    results_df = results_df.sort_values(by=['retweet_count'], ascending=False)
    # print(results_df['tweet_flag'])

    # Segmentation by tweet type
    return segment_by_tweet_type(results_df)

def segment_by_tweet_type(df):
    df_org = df[df['tweet_flag'] == 'original']
    df_quoted = df[df['tweet_flag'] == 'quoted']
    df_retweet = df[df['tweet_flag'] == 'retweeted']
    df_reply = df[df['tweet_flag'] == 'reply']
    return (df_org, df_quoted, df_retweet, df_reply)

user_id = '908326492718764034'
output = fetch_user_results(user_id)
tweet_types = ['original', 'quoted', 'retweet', 'reply']
for idx in range(4):
    print('-------------------------------')
    print(tweet_types[idx])
    print(output[idx])