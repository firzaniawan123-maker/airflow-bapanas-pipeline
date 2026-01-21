from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql+psycopg2://food:food@airflow-food-postgres-1:5432/food_db"
REPO_PATH = "/opt/airflow/dags/food-price-data"

def export_to_csv():
    engine = create_engine(DB_URL)
    # Ambil data dan simpan ke folder foof-price-data
    pd.read_sql("SELECT * FROM sphp_daily_price", engine).to_csv(f"{REPO_PATH}/sphp_data.csv", index=False)
    pd.read_sql("SELECT * FROM milling_daily_price", engine).to_csv(f"{REPO_PATH}/milling_data.csv", index=False)

with DAG(
    dag_id="db_to_github_export",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * *",
    catchup=False
) as dag:

    task_export = PythonOperator(
        task_id="generate_csv",
        python_callable=export_to_csv
    )

    task_push = BashOperator(
        task_id="push_to_github",
        bash_command=f"""
            cd {REPO_PATH} && \
            git config user.email "airflow@example.com" && \
            git config user.name "Airflow Bot" && \
            git add sphp_data.csv milling_data.csv && \
            git commit -m "Update data harian {datetime.now().strftime('%Y-%m-%d %H:%M')}" || echo "No changes" && \
            git push origin main
        """
    )

    task_export >> task_push