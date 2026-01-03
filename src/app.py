import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="NYC Taxi Analytics (DuckDB)", layout="wide")

st.title("🚖 NYC Taxi Big Data Analyzer")
st.markdown("Powered by **DuckDB** - Processing millions of rows in milliseconds.")

@st.cache_resource
def get_connection():
    con = duckdb.connect()
    return con

con = get_connection()
DATA_PATH = os.path.join("data", "taxi_data.parquet")

st.sidebar.header("Filter Options")
try:
    min_dist, max_dist = con.execute(f"SELECT MIN(trip_distance), MAX(trip_distance) FROM '{DATA_PATH}'").fetchone()
    
    distance_filter = st.sidebar.slider(
        "Select Trip Distance (Miles)", 
        float(0), float(50), (float(0), float(20))
    )
except Exception as e:
    st.error(f"Could not load data: {e}")
    st.stop()

st.subheader("🚀 Real-Time KPIs")

query = f"""
    SELECT 
        COUNT(*) as total_trips,
        AVG(total_amount) as avg_fare,
        AVG(trip_distance) as avg_dist,
        AVG(tip_amount) as avg_tip
    FROM '{DATA_PATH}'
    WHERE trip_distance BETWEEN {distance_filter[0]} AND {distance_filter[1]}
"""
metrics = con.execute(query).df()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips", f"{metrics['total_trips'][0]:,.0f}")
col2.metric("Avg Fare", f"${metrics['avg_fare'][0]:.2f}")
col3.metric("Avg Distance", f"{metrics['avg_dist'][0]:.1f} miles")
col4.metric("Avg Tip", f"${metrics['avg_tip'][0]:.2f}")

st.divider()
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Hourly Traffic Trends")
    hourly_query = f"""
        SELECT 
            EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
            COUNT(*) as trip_count
        FROM '{DATA_PATH}'
        WHERE trip_distance BETWEEN {distance_filter[0]} AND {distance_filter[1]}
        GROUP BY hour
        ORDER BY hour
    """
    hourly_data = con.execute(hourly_query).df()
    fig_hourly = px.bar(hourly_data, x='hour', y='trip_count', title="Trips by Hour of Day")
    st.plotly_chart(fig_hourly, use_container_width=True)

with col_right:
    st.subheader("Tip vs. Distance")
    scatter_query = f"""
        SELECT trip_distance, tip_amount 
        FROM '{DATA_PATH}' 
        WHERE trip_distance BETWEEN {distance_filter[0]} AND {distance_filter[1]}
        USING SAMPLE 1000
    """
    scatter_data = con.execute(scatter_query).df()
    fig_scatter = px.scatter(scatter_data, x='trip_distance', y='tip_amount', title="Tip Amount vs. Trip Distance (Sampled)")
    st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()
st.subheader("👨‍💻 SQL Playground")
st.markdown("Write your own DuckDB SQL query below to analyze the data directly.")

user_query = st.text_area("SQL Query", value=f"SELECT passenger_count, AVG(total_amount) FROM '{DATA_PATH}' GROUP BY passenger_count", height=100)

if st.button("Run Query"):
    try:
        import time
        start = time.time()
        result = con.execute(user_query).df()
        end = time.time()
        
        st.success(f"Query executed in {end-start:.4f} seconds")
        st.dataframe(result)
    except Exception as e:
        st.error(f"SQL Error: {e}")