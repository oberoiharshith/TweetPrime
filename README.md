```markdown
# TweetPrime

TweetPrime is a lightweight analytics pipeline that pulls tweets from the Twitter API, stores them in three fit‑for‑purpose databases, and serves low‑latency insights to BI dashboards. Index tuning and read‑through caching cut typical query latency by about 70 percent.

## Features
- Scheduled fetcher for Twitter API v2 (search or filtered stream)
- Polyglot persistence  
  - MySQL for flattened metrics and time‑series queries  
  - MongoDB for full tweet JSON and text search  
  - Neo4j for social‑graph traversal
- FastAPI service with auto‑generated Swagger docs
- Result caching with local pickle files
- Compatible with Power BI and Grafana

## Repository layout
```

TweetPrime/
├── data/                 # Pickled caches
├── src/                  # Core library code
│   ├── cache.py
│   ├── connections.py
│   ├── tweet\_data\_processor.py
│   └── twitter\_queries.py
├── main.py               # CLI entry point
├── config.yml            # Sample runtime config
└── Results.ipynb         # Benchmark notebook

````

## Quick start
1. **Clone and install**
   ```bash
   git clone https://github.com/<your‑org>/tweetprime.git
   cd tweetprime
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
````

2. **Configure credentials**
   Edit `config.yml` or set environment variables for:

   * `TWITTER_BEARER_TOKEN`
   * Database URIs for MySQL, MongoDB, and Neo4j.

3. **Ingest tweets**

   ```bash
   python main.py fetch --hours 2
   ```

4. **Serve the API**

   ```bash
   python main.py serve
   # Swagger UI at http://localhost:8000/docs
   ```

## Requirements

* Python 3.11+
* MySQL 8
* MongoDB 6
* Neo4j 5

