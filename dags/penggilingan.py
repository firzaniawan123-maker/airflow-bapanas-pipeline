"""
DAG: Beras Penggilingan ETL
Deskripsi: Mengambil data harga Beras Medium dan Premium di tingkat Penggilingan (Hulu).
           Data ini digunakan untuk dibandingkan dengan harga SPHP (Hilir) di dashboard.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# Konfigurasi Koneksi Database
DB_URL = "postgresql+psycopg2://food:food@food-postgres:5432/food_db"

# --- 1. EXTRACT ---
def extract_penggilingan_task():
    """
    Mengambil data harga pangan tingkat produsen/penggilingan (level_harga_id=1).
    """
    print("Step 1: Extracting Rice Milling data...")
    url = "https://api-panelhargav2.badanpangan.go.id/api/front/harga-pangan-informasi?province_id=&city_id=&level_harga_id=1"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://panelharga.badanpangan.go.id/"
    }
    
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json().get("data", [])
    
    if not data:
        raise ValueError("Data penggilingan tidak ditemukan")
    
    return data

# --- 2. TRANSFORM ---
def transform_penggilingan_task(ti):
    """
    Filter data Beras Medium & Premium Penggilingan serta menangani harga 0 (today vs yesterday).
    """
    raw_data = ti.xcom_pull(task_ids='extract_penggilingan')
    print("Step 2: Transforming Milling data...")
    
    df = pd.DataFrame(raw_data)
    
    # Filter hanya ID 5 (Medium) dan 6 (Premium)
    target_ids = [5, 6]
    df = df[df['id'].isin(target_ids)].copy()
    
    # Jika 'today' nilainya 0, gunakan harga 'yesterday'. 
    df['final_price'] = df.apply(
        lambda row: row['yesterday'] if row['today'] == 0 else row['today'], 
        axis=1
    )
    
    # Pilih kolom final
    df = df[['name', 'final_price', 'yesterday_date']]
    df.columns = ['commodity_name', 'price', 'price_date']
    
    # Tambahkan metadata
    df['extracted_at'] = pd.Timestamp.now()
    
    return df.to_json(date_format='iso', orient='split')

# --- 3. CREATE TABLE ---
def create_table_penggilingan_task():
    """
    Memastikan tabel 'milling_daily_price' tersedia.
    """
    print("Step 3: Ensuring table 'milling_daily_price' exists...")
    engine = create_engine(DB_URL)
    
    query = """
    CREATE TABLE IF NOT EXISTS milling_daily_price (
        commodity_name VARCHAR(100),
        price FLOAT,
        price_date VARCHAR(20),
        extracted_at TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

# --- 4. LOAD ---
def load_penggilingan_to_db(ti):
    """
    Simpan data ke PostgreSQL dengan metode append.
    """
    json_data = ti.xcom_pull(task_ids='transform_penggilingan')
    df = pd.read_json(json_data, orient='split')
    
    if df.empty:
        print("No data to load.")
        return

    print(f"Step 4: Loading {len(df)} rows to milling_daily_price...")
    engine = create_engine(DB_URL)
    df.to_sql("milling_daily_price", engine, if_exists="append", index=False)
    print("Milling ETL Finished.")

# --- DAG DEFINITION ---
with DAG(
    dag_id="beras_penggilingan_etl",
    description="ETL Harga Beras tingkat Penggilingan (Produsen)",
    start_date=datetime(2026, 1, 19),
    schedule_interval="0 21 * * *", # data biasanya update jam 13.00 (UTC + 7)
    catchup=False,
    tags=["pangan", "produsen", "hulu"],
) as dag:

    task_1 = PythonOperator(
        task_id="extract_penggilingan",
        python_callable=extract_penggilingan_task
    )

    task_2 = PythonOperator(
        task_id="transform_penggilingan",
        python_callable=transform_penggilingan_task
    )

    task_3 = PythonOperator(
        task_id="create_table_milling",
        python_callable=create_table_penggilingan_task
    )

    task_4 = PythonOperator(
        task_id="load_milling_to_db",
        python_callable=load_penggilingan_to_db
    )

    task_1 >> task_2 >> task_3 >> task_4