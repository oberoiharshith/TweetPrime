import json
import logging.config
from datetime import datetime

from src.connections import get_mongodb_conn, get_mysql_conn, get_neo4j_conn
from src.trending_hashtags import TrendingHashtags

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


class TweetDataProcessor:
    def __init__(self):
        self.trending_hashtags = TrendingHashtags()
        self.mysql_conn = get_mysql_conn()
        self.create_user_tb_mysql()
        self.tweet_collection = get_mongodb_conn(collection="tweet_data")
        self.user_collection = get_mongodb_conn(collection="user_data")
        self.neo4j_connection = get_neo4j_conn()
        self.neo4j_connection.execute_query("CREATE INDEX user_id IF NOT EXISTS FOR (n:user) ON (n.id_str)")

    def create_user_tb_mysql(self):
        sql_setup = """CREATE TABLE IF NOT EXISTS users (
                        id_str VARCHAR(255) NOT NULL,
                        name VARCHAR(255),
                        screen_name VARCHAR(255),
                        protected BOOLEAN,
                        verified BOOLEAN,
                        followers_count INT,
                        friends_count INT,
                        listed_count INT,
                        favourites_count INT,
                        statuses_count INT,
                        created_at TIMESTAMP,
                        last_post_timestamp TIMESTAMP,
                        PRIMARY KEY (id_str),
                        INDEX (name),
                        INDEX (screen_name)
                        );
                    """
        try:
            self.mysql_conn.cursor().execute(sql_setup)
            self.mysql_conn.commit()
        except Exception:
            logger.exception(f"Error occurred while creating table userdata in MySQL DB.")

    @staticmethod
    def parse_datetime(timestamp_str):
        return datetime.strftime(datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S %z %Y'), '%Y-%m-%d %H:%M:%S')

    def process_hashtag(self, hashtags: list, tweet_id: str, user_id: str) -> None:
        """
        Function to save hashtags in MySQL db
        Args:
            hashtags: List of hashtags to be saved
            tweet_id: Tweet in which the hashtag was mentioned
            user_id: User who used the hashtag
        Returns: None
        """
        h_list = []
        for hashtag in hashtags:
            logger.info(f"Saving hashtag: {hashtag['text']}")
            self.mysql_conn.cursor().execute(f"""
            INSERT INTO hashtags (hashtag, user_id, tweet_id) VALUES ('{hashtag["text"]}', '{user_id}', '{tweet_id}')
            """)
            h_list.append(hashtag["text"])
        self.mysql_conn.commit()
        self.trending_hashtags.update_hashtags(hashtags=h_list)

    def process_tweet(self, tweet_data: dict) -> None:
        """
        Function to process and save tweet to MongoDB
        Args:
            tweet_data: Dict have tweet data
        Returns: None
        """
        tweet_id = tweet_data['id_str']
        logger.info(f"Processing Tweet: {tweet_id}")
        keys_to_be_dropped = ["id", "geo", "favorited", "retweeted", "filter_level", "quoted_status_id"]
        for key in keys_to_be_dropped:
            tweet_data.pop(key, None)
        tweet_data["user"] = tweet_data["user"]["id_str"]

        # Check if tweet with same ID exists
        existing_tweet = self.tweet_collection.find_one({"id_str": tweet_id})
        if existing_tweet:
            existing_created_at = datetime.strptime(existing_tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
            new_created_at = datetime.strptime(tweet_data['created_at'], '%a %b %d %H:%M:%S %z %Y')

            if new_created_at > existing_created_at:
                # Update existing tweet with new data
                self.tweet_collection.replace_one({"id_str": tweet_id}, tweet_data)
                logger.info(f"Updated existing tweet with ID: {tweet_id}")
            else:
                logger.info(f"Skipping tweet with ID {tweet_id} as existing tweet is newer")
        else:
            # Insert new tweet data
            hashtags = tweet_data.get("entities", {}).get("hashtags")
            if hashtags:
                self.process_hashtag(hashtags=hashtags, tweet_id=tweet_data["id_str"], user_id=tweet_data["user"])
            self.tweet_collection.insert_one(tweet_data)
            logger.info(f"Inserted new tweet with ID: {tweet_id}")

    def process_reply_user_mysql(self, tweet_data: dict) -> None:
        """
        Function to process and save user in_reply_to_user_id_str information into MySQL
        Args:
            tweet_data: Dict have tweet data
        Returns: None
        """
        logger.info(f"Processing User into MySQL: {tweet_data['in_reply_to_user_id_str']}")

        ## For reply users not inserted in database yet, name will be equal to screen_name 
        sql_insertion = f"""
        REPLACE INTO users 
        (id_str, name, screen_name, protected, verified, followers_count,
        friends_count, listed_count, favourites_count, statuses_count,
        created_at, last_post_timestamp
        ) VALUES (
        '{tweet_data['in_reply_to_user_id_str']}', 
        '{tweet_data['in_reply_to_screen_name']}', 
        '{tweet_data['in_reply_to_screen_name']}', 
        null, null, null, null, null, null, null, null, null
        );
        """
        self.mysql_conn.cursor().execute(sql_insertion)
        self.mysql_conn.commit()

    def process_user_mysql(self, tweet_data: dict, user_data: dict) -> None:
        """
        Function to process and save user information into MySQL
        Args:
            tweet_data: Dict have tweet data
            user_data: Dict have user data
        Returns: None
        """
        logger.info(f"Processing User into MySQL: {user_data['id_str']}")

        user_data['name'] = user_data['name'].replace("'", "\\'").replace('"', '\\"')
        user_data['screen_name'] = user_data['screen_name'].replace("'", "\\'").replace('"', '\\"')

        sql_insertion = f"""
        REPLACE INTO users 
        (id_str, 
        name, 
        screen_name, 
        protected, 
        verified, 
        followers_count,
        friends_count, 
        listed_count, 
        favourites_count, 
        statuses_count,
        created_at, 
        last_post_timestamp
        ) VALUES (
        '{user_data['id_str']}', 
        '{user_data['name']}',
        '{user_data['screen_name']}',
        {user_data['protected']}, 
        {user_data['verified']}, 
        {user_data['followers_count']},
        {user_data['friends_count']}, 
        {user_data['listed_count']}, 
        {user_data['favourites_count']},
        {user_data['statuses_count']}, 
        TIMESTAMP('{self.parse_datetime(user_data['created_at'])}'),
        GREATEST(COALESCE(last_post_timestamp,'1000-01-01 00:00:00'), TIMESTAMP('{self.parse_datetime(tweet_data['created_at'])}'))
        );
        """
        self.mysql_conn.cursor().execute(sql_insertion)
        self.mysql_conn.commit()

    def process_user_mongodb(self, user_data: dict) -> None:
        """
        Function to process and save user information into MongoDB
        Args:
            user_data: Dict have user data
        Returns: None
        """
        logger.info(f"Processing User into MongoDB: {user_data['id_str']}")

        user = {'id_str': user_data['id_str'], 'reply_users': [], 'quoted_users': [], 'retweeted_users': [],
                'replied_by_users': [], 'quoted_by_users': [], 'retweeted_by_users': []}

        self.user_collection.update_one({'id_str': user['id_str']},
                                        {'$setOnInsert': user}, upsert=True)

    def set_relationship_mongodb(self, id_A, id_B, field_A, field_B) -> None:
        """
        Function to add users id into relationship lists in MongoDB
        Args:
            id_A: string, user id A
            id_B: string, user id B
            field_A: string, relationship list field name in user id A
            field_B: string, relationship list field name in user id B
        Returns: None
        """
        self.user_collection.update_one({'id_str': id_A},
                                        {'$addToSet': {field_A: id_B}})

        self.user_collection.update_one({'id_str': id_B},
                                        {'$addToSet': {field_B: id_A}})

    def set_relationship_neo4j(self, user_A, user_B, relationship, time, tweet_A, tweet_B) -> None:
        """
        Function to add users relationships into Neo4J

        Args:
            user_A: Dict have user id A data
            user_B: Dict have user id B data
            relationship: relationship between users A and B
            time: datetime of interaction
            tweet_A: tweet id authored by user id A
            tweet_B: tweet id authored by user id B
        Returns: None
        """

        query = f"""MERGE (a:user {" { id_str: '" + user_A['id_str'] + "'}"} )
                    ON CREATE SET a.screen_name = '{user_A['screen_name']}', a.tweet_list = ['{tweet_A}']
                    ON MATCH SET a += {'{'} tweet_list : CASE WHEN '{tweet_A}' IN a.tweet_list THEN a.tweet_list ELSE a.tweet_list + '{tweet_A}' END  {'}'}
                    WITH a
                    MERGE (b:user {" { id_str: '" + user_B['id_str'] + "'}"} )
                    ON CREATE SET b.screen_name = '{user_B['screen_name']}', b.tweet_list = ['{tweet_B}']
                    ON MATCH SET b += {'{'} tweet_list : CASE WHEN '{tweet_B}' IN b.tweet_list THEN b.tweet_list ELSE b.tweet_list + '{tweet_B}' END {'}'}
                    WITH a,b
                    MERGE (a)-[r:{relationship}]->(b)
                    ON CREATE SET r.count = 1, r.last_interaction = '{time}'
                    ON MATCH SET r.count = r.count + 1, 
                    r.last_interaction = CASE r.last_interaction WHEN > '{time}' THEN r.last_interaction ELSE '{time}' END
        """
        self.neo4j_connection.execute_query(query)

    def process_data(self, file_path: str) -> None:
        """
        Function to process tweet data from the JSON file
        Args:
            file_path: Path to JSON file
        Returns: None
        """
        with open(file_path, 'r') as file:
            for line in file:
                if line != '\n':

                    data = json.loads(line)

                    # Process User into MySQL
                    self.process_user_mysql(tweet_data=data, user_data=data['user'])

                    # Process User into MongoDB
                    # self.process_user_mongodb(user_data=data['user'])

                    data["is_retweet_status"] = False

                    if data['in_reply_to_user_id_str'] is not None:
                        # Process User into MySQL
                        self.process_reply_user_mysql(data)

                        # Process User into MongoDB
                        # self.process_user_mongodb(user_data={'id_str': data['in_reply_to_user_id_str']})

                        # Add user ids to relationship lists
                        self.set_relationship_mongodb(id_A=data['user']['id_str'],
                                                      id_B=data['in_reply_to_user_id_str'],
                                                      field_A='reply_users',
                                                      field_B='replied_by_users')

                        # Add Relationship into Neo4J
                        self.set_relationship_neo4j(user_A=data['user'],
                                                    user_B={'id_str': data['in_reply_to_user_id_str'],
                                                            'name': data['in_reply_to_screen_name'],
                                                            'screen_name': data['in_reply_to_screen_name']},
                                                    relationship='replied_to',
                                                    time=self.parse_datetime(data['created_at']),
                                                    tweet_A=data['id_str'],
                                                    tweet_B=data['in_reply_to_status_id_str'])

                    if 'retweeted_status' in data:

                        try:
                            # Process User into MySQL
                            self.process_user_mysql(tweet_data=data['retweeted_status'],
                                                    user_data=data['retweeted_status']['user'])

                            # Process User into MongoDB
                            # self.process_user_mongodb(user_data=data['retweeted_status']['user'])

                            # Add user ids to relationship lists
                            self.set_relationship_mongodb(id_A=data['user']['id_str'],
                                                          id_B=data['retweeted_status']['user']['id_str'],
                                                          field_A='retweeted_users',
                                                          field_B='retweeted_by_users')

                            # Add Relationship into Neo4J                        
                            self.set_relationship_neo4j(user_A=data['user'],
                                                        user_B=data['retweeted_status']['user'],
                                                        relationship='retweeted',
                                                        time=self.parse_datetime(data['created_at']),
                                                        tweet_A=data['id_str'],
                                                        tweet_B=data['retweeted_status']['id_str'])

                            # Process Tweet
                            self.process_tweet(tweet_data=data["retweeted_status"])
                            data["is_retweet_status"] = True
                            data["retweeted_status_id_str"] = data["retweeted_status"]["id_str"]
                            data.pop("retweeted_status")

                        except Exception:
                            logger.exception(f"Error processing retweet")

                    if 'quoted_status' in data:

                        try:
                            # Process User into MySQL
                            self.process_user_mysql(tweet_data=data['quoted_status'],
                                                    user_data=data['quoted_status']['user'])

                            # Process User into MongoDB
                            # self.process_user_mongodb(user_data=data['quoted_status']['user'])

                            # Add user ids to relationship lists
                            self.set_relationship_mongodb(id_A=data['user']['id_str'],
                                                          id_B=data['quoted_status']['user']['id_str'],
                                                          field_A='quoted_users',
                                                          field_B='quoted_by_users')

                            # Add Relationship into Neo4J
                            self.set_relationship_neo4j(user_A=data['user'],
                                                        user_B=data['quoted_status']['user'],
                                                        relationship='quoted',
                                                        time=self.parse_datetime(data['created_at']),
                                                        tweet_A=data['id_str'],
                                                        tweet_B=data['quoted_status']['id_str'])

                            # Process Tweet
                            self.process_tweet(tweet_data=data["quoted_status"])
                            data.pop("quoted_status")

                        except Exception:
                            logger.exception(f"Error processing quoted tweet")

                    self.process_tweet(tweet_data=data)
