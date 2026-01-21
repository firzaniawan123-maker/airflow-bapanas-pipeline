# Beras SPHP Daily ETL (Airflow + PostgreSQL)

Pipeline ETL harian untuk mengambil data **harga Beras SPHP** dari **API Badan Pangan Nasional (Bapanas)**, melakukan transformasi data, dan menyimpannya ke **PostgreSQL** menggunakan **Apache Airflow**.

---

## Tech Stack
- Python 3
- Apache Airflow
- PostgreSQL
- Pandas & NumPy
- Docker & Docker Compose

---

## Data Source
API resmi Badan Pangan Nasional (Bapanas):

# Beras SPHP Daily ETL (Airflow + PostgreSQL)

Pipeline ETL harian untuk mengambil data **harga Beras SPHP** dari **API Badan Pangan Nasional (Bapanas)**, melakukan transformasi data, dan menyimpannya ke **PostgreSQL** menggunakan **Apache Airflow**.

---

## Tech Stack
- Python 3
- Apache Airflow
- PostgreSQL
- Pandas & NumPy
- Docker & Docker Compose

---

## Data Source
API resmi Badan Pangan Nasional (Bapanas):

Extract (API Bapanas)
↓
Transform (Cleaning & Filtering)
↓
Create Table (PostgreSQL)
↓
Load (Append Data)


---

## Output Table

**Table Name:** `sphp_daily_price`

 province_name                 | character varying(100)      |           |          | 
 rata_rata_geometrik           | double precision            |           |          |
 hpp_hap                       | double precision            |           |          |
 hpp_hap_percentage            | double precision            |           |          |
 hpp_hap_percentage_gap_change | character varying(50)       |           |          |
 extracted_at                  | timestamp without time zone |           |          |

---

## DAG Information
- **DAG ID:** `beras_sphp_etl_v2`
- **Schedule:** `0 21 * * *` (Daily)
- **Catchup:** Disabled
- **Executor:** LocalExecutor

---

## Run Project

Build dan jalankan container:

```bash
docker compose build
docker compose up -d


Airflow Web UI:

http://localhost:8080


Login default:

Username: admin
Password: admin

Check Data in PostgreSQL

Masuk ke database:

docker compose exec food-postgres psql -U food -d food_db