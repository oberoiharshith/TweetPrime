import yaml

with open("config.yml") as fp:
    config = yaml.safe_load(fp)
    cache_config = config["cache"]
    mysql_config = config["mysql"]
    mongodb_config = config["mongodb"]
    neo4j_config = config["neo4j"]
    hashtag_config = config["hashtag"]
