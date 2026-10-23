# Real-Time Tech Jobs Scraper Pipeline

An end-to-end, real-time data pipeline designed to scrape tech job postings, stream them through a message broker, filter the data, and store it in a relational database. The entire workflow is orchestrated using Apache Airflow and containerized with Docker for seamless deployment.

## Architecture & Tech Stack

This project utilizes a modern data engineering stack structured as a monorepo:
* **Apache Airflow:** Orchestrates the workflow and schedules the scraping tasks.
* **Scrapy (Python):** Extracts job posting data from target websites.
* **Apache Kafka & Zookeeper:** Acts as the central message broker, handling high-throughput data streams (Topic: `tech_jobs_stream`).
* **Python Consumer:** Listens to the Kafka stream, performs data cleansing/filtering (keeping only tech-related roles), and handles database ingestion.
* **PostgreSQL:** Stores the final, cleaned dataset.
* **Docker & Docker Compose:** Containerizes the Airflow environment, PostgreSQL, Zookeeper, and Kafka to ensure consistent environments across deployments.

## Project Structure

The repository is structured as a monorepo containing both the orchestration environment and the scraping logic:

```text
realtime-tech-job-scrapper/
│
├── airflow/
│   ├── dags/                        # Airflow DAGs (e.g., daily_scrapper_dag.py)
│   ├── docker-compose.yml           # Infrastructure configuration (Airflow, Kafka, Postgres)
│   └── Dockerfile                   # Custom Airflow image build with Scrapy & Kafka dependencies
│
├── scrapper/
│   ├── job_scraper/                 # Scrapy project folder containing spiders and pipelines
│   ├── consumer.py                  # Kafka consumer script for data cleansing and DB ingestion
│   └── scrapy.cfg                   # Scrapy configuration file
│
├── .gitignore
└── README.md
```

## Prerequisites
Before you begin, ensure you have the following installed on your local machine:
* Docker and Docker Compose
* Python 3.8+
* Git

## Setup and Installation
1. Clone the Repository
```bash
git clone [https://github.com/rafiffrhn/realtime-tech-job-scraper.git](https://github.com/rafiffrhn/realtime-tech-job-scraper.git)
```
2. Start the Docker Infrastructure
Navigate to the `airflow` directory to build up and spin up the containers. This will start PostgreSQL, Zookeeper, Kafka, Airflow Webserver, and Airflow Scheduler.
```bash
cd realtime-airflow
docker-compose up -d --build
```
3. Setup Python Virtual Environment (Local)
To run the consumer script locally on your host machine, set up a virtual environment and install the required packages:
```bash
# Return to the root directory
cd ..

# Create and activate virtual environment (Windows)
python -m venv venv
.\venv\Scripts\activate

# Install requirements
pip install psycopg2-binary kafka-python
```

## How to Run the Pipeline
The pipeline operates in two main phases: Data Extraction (Scrapy + Airflow) and Data Ingestion (Kafka Consumer).
## Phase 1: Trigger the Scraper via Airflow
1. Open your web browser and navigate to the Airflow UI at `http://localhost:8081`.
2. Log in using the default credentials configured in the docker-compose.yml.
3. Locate the DAG named `tech_jobs_kafka_pipeline`.
4. Unpause the DAG (toggle the switch to blue) and click the Trigger DAG (play button) to run it manually, or let it run based on its defined schedule.
5. The Scrapy spider will execute inside the Airflow container, scrape the data, and send it to the Kafka broker internally via port `29092`.

## Phase 2: Run the Kafka Consumer
While the scraper is running (or after it finishes, since Kafka retains messages), start the consumer script on your local machine:
```bash
cd realtime-job-scrapper
python consumer.py
```
What the consumer does:
* Connects to Kafka externally via `localhost:9092`.
* Consumes messages from the `tech_jobs_stream` topic.
* Filters the job titles using specific tech keywords (e.g., *data, engineer, scientist, python, machine learning*).
* Ingests the valid records into the PostgreSQL database running on localhost:5433.
* Logs `✅ VALID (Disimpan) for accepted jobs` and `❌ DIBUANG (Bukan Tech) for rejected jobs`.

## Database Schema
The consumer automatically creates the tech_jobs table in PostgreSQL. If it does not exist.
Column Name | Data Type | Description |
--- | --- | --- | 
`id` | SERIAL | Primary Key (Auto-incrementing) |
`title` | VARCHAR(255) | Job Title (e.g., Data Engineer) |
`company`	| VARCHAR(255)	| Hiring Company Name |
`location`	| VARCHAR(255)	| Job Location |
`date_posted`	| VARCHAR(100)	| The date the job was originally posted |

## Network Configuration Details
To allow seamless communication between the Docker containers and the host machine, Kafka is configured with dual listeners:
* Internal Network (Airflow/Scrapy to Kafka): Uses `kafka:29092`. This is configured in the Scrapy `pipelines.py`.
* External Network (Local Windows Host to Kafka): Uses `localhost:9092`. This is configured in the `consumer.py` script.

## Troubleshooting
* Task stays in `up_for_retry or` fails with `ECONNREFUSED`: This happens if the Scrapy DAG is triggered before Kafka is fully ready. Wait 30 seconds after `docker-compose up` before triggering the DAG.
* `FATAL: password authentication failed for user "postgres":` Ensure that your `consumer.py` uses the exact database credentials defined in the `docker-compose.yml` (`DB_USER="airflow"`, `DB_PASS="airflow"`, `DB_NAME="airflow"`).
* Cannot connect to Kafka from `consumer.py`: Verify that you are connecting to `localhost:9092` and not `kafka:9092`, as Windows cannot resolve the Docker container hostname directly.
