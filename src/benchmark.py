import duckdb
import pandas as pd
import time
import os

# Define the path to your data
DATA_PATH = os.path.join("data", "taxi_data.parquet")

def run_pandas_benchmark():
    print(f"Loading {DATA_PATH} with Pandas...")
    start_time = time.time()
    
    # Pandas loads ALL data into RAM first
    df = pd.read_parquet(DATA_PATH)
    
    # Perform aggregation: Average trip distance by passenger count
    result = df.groupby('passenger_count')['trip_distance'].mean()
    
    end_time = time.time()
    print(f"Pandas Result:\n{result.head()}")
    return end_time - start_time

def run_duckdb_benchmark():
    print(f"Querying {DATA_PATH} with DuckDB...")
    start_time = time.time()
    
    # DuckDB queries the file directly (Zero-Copy) without loading everything
    query = f"""
        SELECT 
            passenger_count,
            AVG(trip_distance) as avg_dist
        FROM '{DATA_PATH}'
        GROUP BY passenger_count
    """
    
    # .df() converts the final tiny result to a dataframe for display
    result = duckdb.query(query).df()
    
    end_time = time.time()
    print(f"DuckDB Result:\n{result.head()}")
    return end_time - start_time

if __name__ == "__main__":
    print("--- STARTING BENCHMARK ---")
    
    # Run Pandas
    pandas_time = run_pandas_benchmark()
    print(f"⏱️  Pandas Time: {pandas_time:.4f} seconds")
    print("-" * 30)
    
    # Run DuckDB
    duckdb_time = run_duckdb_benchmark()
    print(f"⏱️  DuckDB Time: {duckdb_time:.4f} seconds")
    print("-" * 30)
    
    # The Verdict
    speedup = pandas_time / duckdb_time
    print(f"🚀 Conclusion: DuckDB was {speedup:.2f}X faster than Pandas!")