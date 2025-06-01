import logging.config
import time

import certifi
import mysql.connector
from neo4j import GraphDatabase
from pymongo import MongoClient

from src.config import mysql_config, mongodb_config, neo4j_config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


def get_mysql_conn(attempts=3, delay=2):
    logger.info("Trying to get MySQL connection")
    attempt = 1
    while attempt < attempts + 1:
        try:
            return mysql.connector.connect(host=mysql_config["host"],
                                           user=mysql_config["user"],
                                           passwd=mysql_config["password"],
                                           database=mysql_config["db"],
                                           port=mysql_config["port"])
        except (mysql.connector.Error, IOError) as err:
            if attempts == attempt:
                logger.exception("Failed to connect to MySQL, exiting without a connection")
                raise err
            logger.info(f"MySQL Connection failed: {err}. Retrying ({attempt}/{attempts - 1})...")
            time.sleep(delay ** attempt)
            attempt += 1


def get_mongodb_conn(collection: str, attempts=3, delay=2):
    logger.info("Trying to get MongoDB connection")
    attempt = 1
    mongo_conn_string = f'mongodb+srv://{mongodb_config["user"]}:{mongodb_config["password"]}@{mongodb_config["host"]}/?retryWrites=true&w=majority&appName=Cluster0'
    while attempt < attempts + 1:
        try:
            client = MongoClient(mongo_conn_string, tlsCAFile=certifi.where())
            db = client[mongodb_config["db"]]
            return db[collection]
        except Exception as err:
            if attempts == attempt:
                logger.exception("Failed to connect to MongoDB, exiting without a connection")
                raise err
            logger.info(f"MongoDB Connection failed: {err}. Retrying ({attempt}/{attempts - 1})...")
            time.sleep(delay ** attempt)
            attempt += 1


def get_neo4j_conn(attempts=3, delay=2):
    logger.info("Trying to get Neo4j connection")
    attempt = 1

    while attempt < attempts + 1:
        try:
            driver = GraphDatabase.driver(neo4j_config["host"], auth=(neo4j_config["user"], neo4j_config["password"]))
            driver.verify_connectivity()
            return driver
        except Exception as err:
            if attempts == attempt:
                logger.exception("Failed to connect to Neo4j, exiting without a connection")
                raise err
            logger.info(f"Neo4j Connection failed: {err}. Retrying ({attempt}/{attempts - 1})...")
            time.sleep(delay ** attempt)
            attempt += 1
