import logging.config
from datetime import datetime, timedelta
from typing import List, Dict

import neo4j
import pandas as pd
import pymongo
import pytz

from src.cache import Cache
from src.config import cache_config
from src.connections import get_mysql_conn, get_mongodb_conn, get_neo4j_conn
from src.trending_hashtags import TrendingHashtags

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


class TwitterQueries:
    def __init__(self):
        self.mysql_connection = get_mysql_conn()
        self.mongo_db = get_mongodb_conn(collection="tweet_data")
        self.neo4j_connection = get_neo4j_conn()

        self.tweet_cache = Cache(cache_path=cache_config["tweet_path"])
        self.user_cache = Cache(cache_path=cache_config["user_path"])

    def ensure_text_index(self):
        if self.mongo_db is not None:
            if 'text' not in self.mongo_db.index_information():
                self.mongo_db.create_index([('text', pymongo.TEXT)], default_language='english')
                logger.info("Text index created on the 'text' field.")
            else:
                logger.info("Text index already exists.")

    def get_user_data_by_username(self, user_name: str, ret_df=False) -> Dict:
        query = "SELECT * FROM users WHERE name LIKE CONCAT('%', %s, '%');"
        with self.mysql_connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (user_name,))
            users_data = cursor.fetchall()
            results = {}
            for user in users_data:
                self.user_cache.put(user["id_str"], user)
                results[user["id_str"]] = user
            return pd.DataFrame(results) if ret_df else results

    def get_user_data(self, user_ids: List[str]) -> Dict:
        """
        Function to get user data by user_ids
        Args:
            user_ids: User ids
        Returns: Dictionary with user_id as key and user data value
        """
        user_data = {}
        cursor = None
        for user_id in user_ids:
            cached_user_data = self.user_cache.get(user_id)
            if cached_user_data:
                user_data[user_id] = cached_user_data
            else:
                if cursor is None:
                    cursor = self.mysql_connection.cursor(dictionary=True)
                query = "SELECT * FROM users WHERE id_str = %s;"
                cursor.execute(query, (user_id,))
                user = cursor.fetchone()
                if user:
                    self.user_cache.put(user_id, user)
                    user_data[user_id] = user
        if cursor:
            cursor.close()
        return user_data

    # Search tweets by username
    def search_tweets_username(self, user_info, time_frame=None):
        # Get the time limit from the utility function, ensuring it is timezone-aware
        time_limit = self.get_time_limit(time_frame) if time_frame else None

        # Build the query with a time limit if specified
        query = {"user": {"$in": list(user_info.keys())}}
        if time_limit:
            query["created_at"] = {"$gte": time_limit.isoformat()}  # Use isoformat for MongoDB compatibility

        results = list(self.mongo_db.find(query))
        if not results:
            logger.info("No tweets found.")
            return pd.DataFrame()

        # Post-process results to add custom fields based on available data
        for result in results:
            self.tweet_cache.put(result["id_str"], result)
            result['is_retweet_status'] = True if result.get('is_retweet_status', False) else False
            result['is_quote_status'] = True if result.get('is_quote_status', False) else False
            result['user_name'] = user_info.get(result['user'], {}).get("name", "Unknown")

        df = pd.DataFrame(results)
        column_order = ['user_name', 'text', 'lang', 'is_retweet_status', 'is_quote_status',
                        'reply_count', 'retweet_count', 'favorite_count', 'created_at']
        df = df[column_order]
        return df

    def create_aggregated_username(self, search_name, sort_metric="created_at",
                                   sort_order=1):  # 1 for ascending, -1 for descending
        user_info = self.get_user_data_by_username(search_name)
        tweets = self.search_tweets_username(user_info)

        data = [{
            'Name': user_info[tweet['user']]["name"],
            'User ID': tweet['user'],
            'Tweet Text': tweet['text'],
            'language': tweet['lang'],
            'Retweet Count': tweet.get('retweet_count', 0),
            'Favorite Count': tweet.get('favorite_count', 0),
            'Reply Count': tweet.get('reply_count', 0),
            'Timestamp': tweet.get('created_at', ''),
            'Quoted Status': tweet.get('is_quoted_status', False),
            'Retweet Status': tweet.get('is_retweet_status', False)
        } for tweet in tweets]

        df = pd.DataFrame(data)
        # Calculate total engagement as the sum of retweets, favorites, and replies
        df['Total Engagement'] = df['Retweet Count'] + df['Favorite Count'] + df['Reply Count']
        sort_column = {'timestamp': 'Timestamp', 'retweet': 'Retweet Count', 'favorite': 'Favorite Count',
                       'engagement': 'Total Engagement'}[sort_metric]
        return df.sort_values(by=[sort_column], ascending=(sort_order == 1))

    # search and sort users based on followers count or last posted timestamp
    def search_and_sort_users(self, search_term, sort_by='followers_count', order='desc'):
        order_by = "DESC" if order == 'desc' else "ASC"
        query = f"""
        SELECT *
        FROM users 
        WHERE name LIKE CONCAT('%', %s, '%') 
        ORDER BY {sort_by} {order_by};
        """
        with self.mysql_connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, (search_term,))
            rows = cursor.fetchall()
            for row in rows:
                self.user_cache.put(row["id_str"], row)
            df = pd.DataFrame(rows)
            return df

    def get_time_limit(self, time_frame):
        """ Calculate the starting date and time for a given time frame with timezone-aware datetime objects. """
        utc = pytz.utc
        now = datetime.now(utc)  # Get the current time in UTC
        if time_frame == '1day':
            return now - timedelta(days=1)
        elif time_frame == '1week':
            return now - timedelta(weeks=1)
        elif time_frame == '1month':
            return now - timedelta(days=30)
        else:
            return None

    def fetch_tweet_ids_from_mysql(self, hashtag):
        with self.mysql_connection.cursor() as cursor:
            query = "SELECT tweet_id FROM hashtags WHERE hashtag LIKE %s"
            cursor.execute(query, (hashtag,))
            tweet_ids = [item[0] for item in cursor.fetchall()]
            return tweet_ids

    def search_tweets_by_hashtag(self, hashtag):
        tweet_ids = self.fetch_tweet_ids_from_mysql(hashtag)
        if not tweet_ids:
            logger.info("No tweet IDs found for hashtag:", hashtag)
            return pd.DataFrame()  # Return an empty DataFrame if no IDs are found

        tweets = self.fetch_tweets_from_mongodb(tweet_ids)  # Make sure to pass tweet_ids here, not query
        if not tweets:
            logger.info("No tweets found in MongoDB matching the tweet IDs from MySQL.")
            return pd.DataFrame()

        data = [{
            'User ID': tweet.get('user', 'Unknown'),
            'Tweet Text': tweet.get('text', ''),
            'Retweet Count': tweet.get('retweet_count', 0),
            'Favorite Count': tweet.get('favorite_count', 0),
            'Reply Count': tweet.get('reply_count', 0),
            'Timestamp': tweet.get('created_at', '')
        } for tweet in tweets]

        return pd.DataFrame(data)

    def fetch_tweets_from_mongodb(self, tweet_ids):
        # Check if tweet_ids list is empty to avoid MongoDB errors
        if not tweet_ids:
            logger.info("No tweet IDs provided to fetch from MongoDB.")
            return []
        tweets = []
        for tweet_id in tweet_ids:
            cached_tweet = self.tweet_cache.get(tweet_id)
            if cached_tweet:
                tweets.append(cached_tweet)
            else:
                tweet_from_mongodb = self.mongo_db.find_one({"id_str": tweet_id})
                if tweet_from_mongodb:
                    # Update cache with fetched tweet
                    self.tweet_cache.put(tweet_id, tweet_from_mongodb)
                    tweets.append(tweet_from_mongodb)
        return tweets

    # Popular tweets based on engagement metrics(Top 10)
    def search_popular_tweets_based_on_engagement(self, time_frame=None):
        time_limit = self.get_time_limit(time_frame) if time_frame else None
        pipeline = [
            {
                '$match': {"created_at": {"$gte": time_limit.isoformat()}} if time_limit else {}
            },
            {
                '$project': {
                    'text': 1,
                    'user': 1,
                    'quote_count': {'$ifNull': ['$quote_count', 0]},
                    'reply_count': {'$ifNull': ['$reply_count', 0]},
                    'retweet_count': {'$ifNull': ['$retweet_count', 0]},
                    'favorite_count': {'$ifNull': ['$favorite_count', 0]},
                    'total_engagement': {
                        '$add': [
                            '$quote_count', '$reply_count', '$retweet_count', '$favorite_count'
                        ]
                    }
                }
            },
            {
                '$sort': {'total_engagement': -1}
            },
            {
                '$limit': 10
            }
        ]

        try:
            results = list(self.mongo_db.aggregate(pipeline))
            if not results:
                logger.info("No tweets found with high engagement.")
                return pd.DataFrame()

            return pd.DataFrame(results)
        except pymongo.errors.OperationFailure:
            logger.exception(f"Error fetching tweets based on engagement")

    def search_tweets_by_keyword(self, keyword, time_frame=None):
        self.ensure_text_index()
        time_limit = self.get_time_limit(time_frame) if time_frame else None

        query = {'$text': {'$search': keyword}}
        if time_limit:
            query['created_at'] = {'$gte': time_limit.isoformat()}
        # Execute the query
        results = list(self.mongo_db.find(query))
        for tweet in results:
            self.tweet_cache.put(tweet["id_str"], tweet)
        if not results:
            logger.info(f"No tweets found containing the keyword '{keyword}' within the specified time frame.")
            return pd.DataFrame()

        # Create a DataFrame from the results
        df = pd.DataFrame(results)
        return df

    def get_relevant_users_by_user_id(self, user_id, limit=10, include_tweet=False):
        text = ''
        if include_tweet:
            text = ', b.tweet_list as tweet_list'
        query = f"""MATCH (a)-[]->(n:user {" { id_str: '" + user_id + "'}"} ) WHERE a.id_str <> "{user_id}"
                WITH a MATCH (a)-[r]->(b) WHERE b.id_str <> "{user_id}"
                RETURN b.screen_name as screen_name, b.id_str as id_str, SUM(SIZE(b.tweet_list)) as n_of_tweets, 
                SUM(r.count) as n_of_interactions, MAX(r.last_interaction) as last_interaction_dt {text}
                ORDER BY n_of_interactions DESC, last_interaction_dt DESC, n_of_tweets DESC
                LIMIT {limit}
                """
        df_user = self.neo4j_connection.execute_query(query, result_transformer_=neo4j.Result.to_df)
        if df_user.shape[0] == 0:
            logger.info('No relevant users found.')
            return pd.DataFrame()
        df_mysql = pd.DataFrame(self.get_user_data(df_user['id_str'].to_list()))
        df_user = df_user.merge(df_mysql.transpose().reset_index().drop(
            columns = ['index','screen_name']), on ='id_str')
        return df_user

    def get_relevant_tweets_by_user_id(self, user_id, limit=10, user_limit=10):
        df_tweet = self.get_relevant_users_by_user_id(user_id, limit=user_limit, include_tweet=True)
        if df_tweet.shape[0] == 0:
            logger.info('No relevant tweets found.')
            return []
        else:
            tweet_list = df_tweet['tweet_list'].sum()[0:limit]
            res = self.fetch_tweets_from_mongodb(tweet_list)
            if len(res) == 0:
                logger.info('No relevant tweets found.')
            return res

    @staticmethod
    def get_trending_hashtags():
        hashtags = TrendingHashtags()
        return hashtags.get_top_hashtags()
