import logging.config
import os.path
import pickle
import time
from typing import Union

from apscheduler.schedulers.background import BackgroundScheduler

from src.config import cache_config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


class Cache:

    def __init__(self, cache_path: str, max_size=cache_config["max_size"]):
        self._data = {}
        self._cache_path = cache_path
        self._max_size = max_size

        self.load_from_disk()
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.save_to_disk, 'interval', seconds=cache_config["save_to_disk_interval"])
        self.scheduler.start()

    def load_from_disk(self) -> None:
        """
        Function to load cache from disk
        """
        if os.path.isfile(self._cache_path):
            logger.debug(f"Loading cache from {self._cache_path}")
            with open(self._cache_path, 'rb') as fp:
                self._data = pickle.load(fp)

    def save_to_disk(self) -> None:
        """
        Function to save cache to disk
        """
        logger.debug(f"Saving cache to {self._cache_path}")
        with open(self._cache_path, 'wb') as fp:
            pickle.dump(self._data, fp)

    def get(self, key: Union[int, str]) -> Union[None, str, dict, int, float, bool]:
        """
        Function to get data from cache
        Args:
            key: Key to be fetched
        Returns:
            Data corresponding to key
        """
        if key in self._data:
            timestamp, data = self._data.get(key)
            if time.time() > timestamp + cache_config["ttl"]:
                logger.debug(f"{key} expired in cache")
                del self._data[key]
                return None
            return data
        logger.debug(f"{key} not found in cache")
        return None

    def put(self, key: Union[int, str], value: Union[str, dict, int, float, bool]) -> None:
        """
        Function to add data to cache
        Args:
            key: Key to be store
            value: Value corresponding to key
        """
        self._data.pop(key, None)
        if len(self._data) > self._max_size:
            logger.debug("Cache full. Deleting oldest entry.")
            oldest_key = next(iter(self._data))
            del self._data[oldest_key]
        self._data[key] = (time.time(), value)

    def __del__(self):
        self.scheduler.shutdown()
        logger.info("Scheduler shut down successfully.")
        self.save_to_disk()
