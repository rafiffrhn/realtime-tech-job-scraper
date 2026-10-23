import scrapy
from job_scraper.items import JobScraperItem

class JobsSpider(scrapy.Spider):
    name = 'jobs_spider'
    allowed_domains = ['realpython.github.io']
    start_urls = ['https://realpython.github.io/fake-jobs/']

    def parse(self, response):
        # Mencari semua elemen kartu lowongan kerja (HTML class: card-content)
        job_cards = response.css('div.card-content')
        
        for job in job_cards:
            item = JobScraperItem()
            
            # Mengekstrak teks dari elemen HTML spesifik
            item['title'] = job.css('h2.title::text').get().strip()
            item['company'] = job.css('h3.company::text').get().strip()
            item['location'] = job.css('p.location::text').get().strip()
            item['date_posted'] = job.css('time::text').get().strip()
            
            # Mengirim data ke Pipeline (nanti ditangkap oleh Kafka)
            yield item