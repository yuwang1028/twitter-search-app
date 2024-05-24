import pandas as pd
import sys
import os
import json
from datetime import datetime
import numpy as np

from utils import connect_to_mysql, connect_to_mongodb

def top_users_by_followers(SQL_client):
    query='''SELECT u.*
            FROM user_profile u
			ORDER BY u.followers_count DESC LIMIT 10'''
    results_df=pd.read_sql_query(query,con=SQL_client)
    return(results_df)

def top_tweets_by_retweets(SQL_client, NoSQL_client):
    query1 = '''
        SELECT tweet_id, COUNT(retweet_id) as retweet_count           
        FROM retweets
        GROUP BY tweet_id
        ORDER BY retweet_count DESC
    '''
    df1 = pd.read_sql_query(query1, con=SQL_client)
    if df1.empty:
        return df1
    
    filtered_tweet_ids = [str(x) for x in df1['tweet_id'].tolist()]
    
    # MongoDB Query
    if filtered_tweet_ids:
        response = NoSQL_client['twitter']['nonrelational'].find(
            {"id_str": {"$in": filtered_tweet_ids}},
            {
                "id_str": 1,
                "text": 1,
                "entities.hashtags.text": 1,
                "possibly_sensitive": 1,
                "entities.media.type": 1,
                "entities.media.url": 1
            }
        )
        df2 = pd.DataFrame(list(response))
        
    if df2.empty:
        return df2
    
    # Data transformation remains mostly unchanged
    # Extracting fields from nested documents
    # df2['hashtags'] = df2['entities'].apply(lambda x: [h['text'] for h in x['hashtags']] if 'hashtags' in x else np.nan)
    # df2['media_type'] = df2['entities'].apply(lambda x: x['media'][0]['type'] if 'media' in x else np.nan)
    # df2['media_url'] = df2['entities'].apply(lambda x: x['media'][0]['url'] if 'media' in x else np.nan)

    # Final merge and sorting
    results_df = pd.merge(df1, df2, left_on='tweet_id', right_on='id_str', how='inner')
    results_df = results_df.sort_values(by='retweet_count', ascending=False)
    return results_df.head(10)



def fetch_metric_results(option):
    SQL_client=connect_to_mysql()
    NoSQL_client=connect_to_mongodb()
    if(option=='1'):
        return(top_users_by_followers(SQL_client))
    elif(option=='2'):
        return(top_tweets_by_retweets(SQL_client,NoSQL_client))
