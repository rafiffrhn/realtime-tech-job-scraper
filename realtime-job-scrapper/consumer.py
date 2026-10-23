import json
import psycopg2
from kafka import KafkaConsumer

# 1. Konfigurasi Koneksi PostgreSQL (Sesuaikan)
DB_HOST = "localhost"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow" # GANTI DENGAN PASSWORD POSTGRESQL

def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tech_jobs (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            company VARCHAR(255),
            location VARCHAR(255),
            date_posted VARCHAR(100)
        )
    """)

def run_consumer():
    # Buka koneksi ke database
    conn = psycopg2.connect(host=DB_HOST, port="5433", database=DB_NAME, user=DB_USER, password=DB_PASS)
    cursor = conn.cursor()
    
    # Siapkan tabel
    create_table_if_not_exists(cursor)
    conn.commit()

    # 2. Buka Koneksi ke Kafka
    print("🎧 Menunggu aliran data dari Kafka topic 'tech_jobs_stream'...")
    consumer = KafkaConsumer(
    'tech_jobs_stream',
    bootstrap_servers=['localhost:9092'], # <--- GANTI INI
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

    # 3. Proses Ingestion (Streaming ke Database dengan Cleansing)
    try:
        # Tentukan kata kunci sakti untuk Tech Jobs
        tech_keywords = ['data', 'engineer', 'scientist', 'analyst', 'python', 'developer', 'software', 'machine learning', 'sql']

        for message in consumer:
            job = message.value
            title_lower = job.get('title', '').lower()
            
            # Logika Cleansing: Cek apakah ada kata kunci tech di dalam judul
            is_tech_job = any(keyword in title_lower for keyword in tech_keywords)

            if is_tech_job:
                # Jika valid, masukkan ke PostgreSQL
                cursor.execute(
                    "INSERT INTO tech_jobs (title, company, location, date_posted) VALUES (%s, %s, %s, %s)",
                    (job.get('title'), job.get('company'), job.get('location'), job.get('date_posted'))
                )
                conn.commit()
                print(f"✅ VALID (Disimpan): {job.get('title')} di {job.get('company')}")
            else:
                # Jika bukan tech job, buang dari pipeline
                print(f"❌ DIBUANG (Bukan Tech): {job.get('title')}")
            
    except KeyboardInterrupt:
        print("Mematikan Consumer...")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_consumer()