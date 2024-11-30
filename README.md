# PostgresQueryCache-DuckDB
PostgresQueryCache-DuckDB
![output](https://github.com/user-attachments/assets/d0cf1822-4112-4b27-a6f8-f34f7d66af11)

# Read more 
https://www.linkedin.com/pulse/speed-up-your-analytics-leveraging-duckdb-lru-cache-postgresql-shah-gluse/?trackingId=fMdF1wz1SAeJBB9BpUAmpA%3D%3D

# Code 
```
from collections import OrderedDict
import duckdb
import psycopg2
from datetime import datetime, timedelta
import hashlib
import os


class LRUCache:
    def __init__(self, max_size_mb, duckdb_path, postgres_conn_params, ttl=120):  # ttl in seconds
        self.max_size_mb = max_size_mb
        self.current_size_mb = 0
        self.cache = OrderedDict()
        self.duckdb_path = duckdb_path
        self.duckdb = duckdb.connect(database=self.duckdb_path)
        self.postgres_conn = psycopg2.connect(**postgres_conn_params)
        self.postgres_cursor = self.postgres_conn.cursor()
        self.ttl = ttl  # Time to live in seconds
        self.create_table_if_not_exists()
        self.check_and_manage_duckdb_size()

    def create_table_if_not_exists(self):
        try:
            self.duckdb.execute("""
                CREATE TABLE IF NOT EXISTS cached_queries (
                    query_hash TEXT,
                    query TEXT,
                    result TEXT,
                    timestamp TIMESTAMP
                )
            """)
            self.duckdb.commit()
        except Exception as e:
            print(f"Error creating table in DuckDB: {e}")

    def get(self, query):
        query_hash = self.hash_query(query)

        # Check if query is in DuckDB cache
        result = self.get_from_duckdb(query_hash)

        if result is not None:
            # Check if the result in DuckDB is not expired
            duckdb_timestamp = self.get_timestamp_from_duckdb(query_hash)
            if duckdb_timestamp and datetime.now() - duckdb_timestamp < timedelta(seconds=self.ttl):
                print(f"DuckDB Cache Hit: Query '{query}' served from DuckDB cache.")
                if self.add_to_cache(query_hash, result):
                    return result
                else:
                    print("Cache is full, cannot add new item.")
                    return result
            else:
                # If expired in DuckDB, remove it and query Postgres
                self.remove_from_duckdb(query_hash)
                print(f"DuckDB Cache Miss: Query '{query}' expired in DuckDB, querying Postgres.")

        # Query Postgres if not in cache or expired
        self.postgres_cursor.execute(query)
        result = self.postgres_cursor.fetchall()
        if self.add_to_cache(query_hash, result):
            # Store in DuckDB for future queries
            self.store_in_duckdb(query_hash, query, result)
            print(f"Postgres Call: Query '{query}' served from Postgres.")
            return result
        else:
            print("Cache is full, cannot add new item.")
            return result

    def add_to_cache(self, query_hash, result):
        # Calculate the size of the result
        result_size_mb = len(str(result).encode()) / (1024 * 1024)
        if self.current_size_mb + result_size_mb > self.max_size_mb:
            # Evict items until there is enough space
            while self.current_size_mb + result_size_mb > self.max_size_mb and self.cache:
                key, (timestamp, value) = self.cache.popitem(last=False)
                self.current_size_mb -= len(str(value).encode()) / (1024 * 1024)
                print(f"Evicted item from cache: {key}")
        self.cache[query_hash] = (datetime.now(), result)
        self.current_size_mb += result_size_mb
        return True

    def get_from_duckdb(self, query_hash):
        try:
            self.duckdb.execute("SELECT result FROM cached_queries WHERE query_hash = ?", [query_hash])
            result = self.duckdb.fetchone()
            if result:
                return result[0]
            else:
                return None
        except Exception as e:
            print(f"Error retrieving query from DuckDB: {e}")
            return None

    def get_timestamp_from_duckdb(self, query_hash):
        try:
            self.duckdb.execute("SELECT timestamp FROM cached_queries WHERE query_hash = ?", [query_hash])
            timestamp = self.duckdb.fetchone()
            if timestamp:
                return timestamp[0]
            else:
                return None
        except Exception as e:
            print(f"Error retrieving timestamp from DuckDB: {e}")
            return None

    def store_in_duckdb(self, query_hash, query, result):
        try:
            # Insert the query and result into DuckDB
            self.duckdb.execute("INSERT INTO cached_queries VALUES (?, ?, ?, ?)",
                                [query_hash, query, str(result), datetime.now()])
            self.duckdb.commit()
            print("Successfully stored query in DuckDB")
            self.check_and_manage_duckdb_size()
        except Exception as e:
            print(f"Error storing query in DuckDB: {e}")

    def remove_from_duckdb(self, query_hash):
        try:
            self.duckdb.execute("DELETE FROM cached_queries WHERE query_hash = ?", [query_hash])
            self.duckdb.commit()
            print("Removed expired query from DuckDB.")
        except Exception as e:
            print(f"Error removing query from DuckDB: {e}")

    def check_and_manage_duckdb_size(self):
        # Get the current size of the DuckDB database file
        db_size_mb = os.path.getsize(self.duckdb_path) / (1024 * 1024)
        print("db_size_mb", db_size_mb)
        if db_size_mb > self.max_size_mb:
            # Remove older queries to reduce the size
            self.remove_older_queries_from_duckdb()

    def remove_older_queries_from_duckdb(self):
        try:
            # Get the oldest queries
            self.duckdb.execute("SELECT query_hash FROM cached_queries ORDER BY timestamp ASC LIMIT 10")
            oldest_queries = [row[0] for row in self.duckdb.fetchall()]
            # Delete the oldest queries
            for query_hash in oldest_queries:
                self.duckdb.execute("DELETE FROM cached_queries WHERE query_hash = ?", [query_hash])
            self.duckdb.commit()
            print("Removed older queries from DuckDB to manage size.")
        except Exception as e:
            print(f"Error removing older queries from DuckDB: {e}")

    def hash_query(self, query):
        return hashlib.sha256(query.encode()).hexdigest()

    def close(self):
        self.duckdb.close()
        self.postgres_cursor.close()
        self.postgres_conn.close()


# Example usage
postgres_conn_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

cache = LRUCache(500, './default.duckdb', postgres_conn_params)
result = cache.get("SELECT count(*) FROM public.users LIMIT 10")
print(result)

# result = cache.get("SELECT * FROM public.users LIMIT 10")
# print(result)
cache.close()

```
![Screenshot 2024-11-29 at 9 53 44 PM](https://github.com/user-attachments/assets/4cf53083-b948-4bcd-b338-3b33b6e92838)




