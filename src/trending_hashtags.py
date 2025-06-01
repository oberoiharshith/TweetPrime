import logging.config
import os
import pickle
from collections import defaultdict
from copy import deepcopy
from typing import List, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from indexed_priority_queue import IndexedPriorityQueue

from src.config import hashtag_config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


class TrendingHashtags:
    def __init__(self):
        self.file_path = hashtag_config["path"]
        if not self.load_trending_hashtags():
            self.hashtag_freq = defaultdict(int)
            self.pq = IndexedPriorityQueue()

        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.save_trending_hashtags, 'interval', seconds=hashtag_config["save_to_disk_interval"])
        self.scheduler.start()

    def update_hashtags(self, hashtags: List[str]) -> None:
        """
        Function to update hashtags count
        Args:
            hashtags: List of hashtags found in tweet
        Returns: None
        """
        for h in hashtags:
            hashtag = h.lower()
            hashtag_count = self.hashtag_freq[hashtag] = self.hashtag_freq[hashtag] + 1

            if hashtag in self.pq:
                self.pq.update(hashtag, hashtag_count)
            elif len(self.pq) < hashtag_config["max_size"]:
                self.pq.push(hashtag, hashtag_count)
            else:
                top_hashtag, top_count = self.pq.peek()
                if hashtag_count > top_count:
                    self.pq.pop()
                    self.pq.push(hashtag, hashtag_count)

    def get_top_hashtags(self) -> List[Tuple[str, int]]:
        """
        Function to get top trending hashtags
        Returns: List of top hashtags and their count
        """
        logger.info(f"Retrieving top {hashtag_config['max_size']} hashtags")
        top_hashtags = []
        temp_heap = deepcopy(self.pq)
        while temp_heap:
            hashtag, count = temp_heap.pop()
            top_hashtags.append((hashtag, count))
        top_hashtags.reverse()
        return top_hashtags

    def save_trending_hashtags(self) -> None:
        """
        Function to save hashtag data to disk
        Returns: None
        """
        logger.info(f"Saving trending hashtags data to {self.file_path}")
        data = {
            "hashtag_freq": self.hashtag_freq,
            "pq": self.pq
        }
        with open(self.file_path, 'wb') as fp:
            pickle.dump(data, fp)

    def load_trending_hashtags(self) -> bool:
        """
        Function to load hashtag data from disk
        Returns: Boolean flag indicating whether data was loaded from disk
        """
        if os.path.isfile(self.file_path):
            logger.debug(f"Loading trending hashtags data from {self.file_path}")
            with open(self.file_path, 'rb') as fp:
                data = pickle.load(fp)
                self.hashtag_freq = data["hashtag_freq"]
                self.pq = data["pq"]
                return True
        logger.info(f"Trending hashtags data not found at {self.file_path}")
        return False

    def __del__(self):
        self.scheduler.shutdown()
        logger.info("Scheduler shut down successfully.")
        self.save_trending_hashtags()
