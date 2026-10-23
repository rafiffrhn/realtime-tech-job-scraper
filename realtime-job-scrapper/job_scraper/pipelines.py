import json
from kafka import KafkaProducer

class KafkaPipeline:
    def __init__(self):
        # Membuka koneksi ke server Kafka lokal saat spider mulai berjalan
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def process_item(self, item, spider):
        # Konversi objek Scrapy Item menjadi dictionary biasa
        job_data = dict(item)
        
        # Menembakkan data ke topic 'tech_jobs_stream'
        self.producer.send('tech_jobs_stream', job_data)
        
        return item

    def close_spider(self, spider):
        # Menutup koneksi dengan aman saat spider selesai
        self.producer.flush()
        self.producer.close()