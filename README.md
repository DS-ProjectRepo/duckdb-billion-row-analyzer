# 🚀 DuckDB vs Pandas Benchmark: NYC Taxi Data

## 📌 Overview
This project benchmarks the performance of **DuckDB (OLAP)** against **Pandas** for large-scale data aggregation. 
Using the NYC Taxi Trip dataset (Parquet format), we simulate a real-world Data Engineering task: calculating average trip distance grouped by passenger count.

## 💻 Hardware Environment
- **CPU:** Ryzen 5 4600H
- **RAM:** 16GB
- **GPU:** GTX 1650 (Not used for this CPU-bound task)

## 📊 Benchmark Results
| Library | Execution Time | Speedup Factor |
| :--- | :--- | :--- |
| **Pandas** | 0.50s | 1x |
| **DuckDB** | **0.03s** | **16.68x Faster** |

> **Note:** DuckDB achieves this speed by using **vectorized execution** and processing the Parquet file directly (Zero-Copy), whereas Pandas must load the entire dataset into RAM first.

## 🛠️ Tech Stack
- **Language:** Python 3.12
- **Engine:** DuckDB (In-process SQL OLAP DBMS)
- **Data Format:** Apache Parquet

## ⚡ How to Run
1. Clone the repository:
   ```bash
   git clone [https://github.com/YOUR_USERNAME/duckdb-billion-row-analyzer.git](https://github.com/YOUR_USERNAME/duckdb-billion-row-analyzer.git)