"""
DAG: Beras SPHP Daily ETL
Deskripsi: Mengambil data spasial harga Beras SPHP dari API Badan Pangan Nasional (Bapanas), 
           melakukan transformasi data, dan menyimpannya ke PostgreSQL untuk kebutuhan dashboard.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

# Konfigurasi Koneksi Database (Best practice: Gunakan Airflow Connections untuk produksi)
DB_URL = "postgresql+psycopg2://food:food@food-postgres:5432/food_db"

# --- 1. EXTRACT ---
def extract_task():
    """
    Mengambil data mentah dari API Badan Pangan Nasional.
    Endpoint: harga-peta-provinsi (Level Harga 3: Eceran)
    Komoditas: 109 (Beras SPHP)
    """
    print("Step 1: Extracting data from Bapanas API...")
    
    # Format tanggal dinamis untuk URL (dd/mm/yyyy)
    today = datetime.now().strftime('%d%%2F%m%%2F%Y')
    url = f"https://api-panelhargav2.badanpangan.go.id/api/front/harga-peta-provinsi?level_harga_id=3&komoditas_id=109&period_date={today}%20-%20{today}"
    
    # Header wajib untuk menghindari blokir (401 Unauthorized)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://panelharga.badanpangan.go.id/"
    }
    
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json().get("data", [])
    
    if not data:
        raise ValueError("API Response empty: Data mungkin belum tersedia saat ini.")
    
    return data

# --- 2. TRANSFORM ---
def transform_task(ti):
    """
    Melakukan pembersihan data:
    1. Filter hanya komoditas Beras SPHP (ID 109).
    2. Konversi tipe data string ke numerik.
    3. Handling data kosong (-) dari API.
    4. Penambahan metadata timestamp extraction.
    """
    # Mengambil data dari task 'extract_data' menggunakan XCom
    raw_data = ti.xcom_pull(task_ids='extract_data')
    
    print("Step 2: Transforming data with Pandas...")
    df = pd.DataFrame(raw_data)
    
    # Filter dan pemilihan kolom krusial untuk dashboard
    df = df[df['commodity_id'] == '109'].copy()
    cols_to_keep = [
        'province_name', 
        'rata_rata_geometrik', 
        'hpp_hap', 
        'hpp_hap_percentage', 
        'hpp_hap_percentage_gap_change'
    ]
    df = df[cols_to_keep]
    
    # DATA CLEANING LOGIC:
    # Mengganti karakter '-' dengan NaN agar kolom menjadi bertipe float
    df['rata_rata_geometrik'] = pd.to_numeric(df['rata_rata_geometrik'].replace('-', np.nan))
    
    # Membersihkan titik pada string harga (misal "13.500" -> 13500) lalu konversi ke numeric
    df['hpp_hap'] = df['hpp_hap'].astype(str).str.replace('.', '', regex=False)
    df['hpp_hap'] = pd.to_numeric(df['hpp_hap'], errors='coerce')
    
    # Metadata: Waktu penarikan data untuk tracking historis
    df['extracted_at'] = pd.Timestamp.now()
    
    # Serialisasi DataFrame ke JSON agar bisa dilewatkan via XCom
    return df.to_json(date_format='iso', orient='split')

# --- 3. CREATE TABLE ---
def create_table_task():
    """
    Memastikan tabel target sudah tersedia di PostgreSQL sebelum proses Load.
    Menggunakan DDL SQL mentah via SQLAlchemy.
    """
    print("Step 3: Ensuring target table exists in PostgreSQL...")
    engine = create_engine(DB_URL)
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sphp_daily_price (
        province_name VARCHAR(100),
        rata_rata_geometrik FLOAT,
        hpp_hap FLOAT,
        hpp_hap_percentage FLOAT,
        hpp_hap_percentage_gap_change VARCHAR(50),
        extracted_at TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

# --- 4. LOAD ---
def load_to_postgres(ti):
    """
    Memasukkan data yang telah bersih ke dalam tabel PostgreSQL.
    Metode: Append (Menambahkan data baru tanpa menghapus data lama).
    """
    # Mengambil data dari task 'transform_data' menggunakan XCom
    json_data = ti.xcom_pull(task_ids='transform_data')
    df = pd.read_json(json_data, orient='split')

    if df.empty:
        print("No data available to load.")
        return
    
    print(f"Step 4: Loading {len(df)} rows to PostgreSQL database...")
    engine = create_engine(DB_URL)

    # Load ke database menggunakan SQLAlchemy engine
    df.to_sql(
        "sphp_daily_price",
        engine,
        if_exists="append",
        index=False
    )
    print("ETL Process Completed Successfully.")

# --- DAG DEFINITION ---
with DAG(
    dag_id="beras_sphp_etl_v2",
    description="Pipeline ETL harian untuk memantau harga Beras SPHP Nasional",
    start_date=datetime(2026, 1, 19),
    schedule_interval="0 21 * * *", # data biasanya update jam 13.00 (UTC + 7)
    catchup=False,
    tags=["pangan", "sphp", "postgres", "data_engineering"],
) as dag:

    # Task 1: Extraction
    task_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task
    )

    # Task 2: Transformation
    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task
    )

    # Task 3: Database Preparation
    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_task
    )

    # Task 4: Loading
    task_load = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_postgres
    )

    # Orchestration: Alur kerja ETL
    task_extract >> task_transform >> task_create_table >> task_load