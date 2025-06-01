from src.tweet_data_processor import TweetDataProcessor
from src.twitter_queries import TwitterQueries

# obj = TweetDataProcessor()
# obj.process_data(file_path="/Users/rohitvernekar/Desktop/corona-out-2")

twitter_queries = TwitterQueries()

user = twitter_queries.get_user_data_by_username("NUFF")

# Search for tweets by the user from the last week
twitter_queries.search_tweets_username(user, '1week')

twitter_queries.search_and_sort_users('bob', sort_by='followers_count', order='desc')

twitter_queries.search_popular_tweets_based_on_engagement('1month')

twitter_queries.search_tweets_by_keyword("death", '1week')

twitter_queries.get_relevant_users_by_user_id("10228272")

twitter_queries.get_relevant_tweets_by_user_id("10228272")

twitter_queries.get_trending_hashtags()

# import time
# from src.twitter_queries import TwitterQueries
#
#
# def time_it(func, *args):
#     start = time.time()
#     res = func(*args)
#     print("Time Taken:", time.time() - start)
#     print(res)
#
#
# tq = TwitterQueries()
#
# time_it(tq.get_trending_hashtags)
#
# time_it(tq.search_tweets_by_hashtag, "covid_19")
#
# time_it(tq.get_relevant_users_by_user_id, "10228272")