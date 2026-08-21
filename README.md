<img width="721" height="271" alt="ETL" src="https://github.com/user-attachments/assets/0199bbd8-3c97-4e53-a8e3-98c2801308bd" />

# olist-de-pipeline

Just a personal data pipeline project I built to play around with some common open sourcedata engineering tools. 
It does batch processing from some APIs and static files.

## What this does

- Ingests raw CSV data using Spark
- Transforms it into a star schema using dbt
- Orchestrates everything with Airflow
- Runs everything in Docker containers

## Tools I used

- **Airflow** - for scheduling and orchestrating the pipeline
- **Spark** - for data ingestion and processing  
- **dbt** - for SQL transformations
- **PostgreSQL** - for data storage
- **Docker** - to containerize everything
- **Jupyter** - for exploring the data

## What I learned
- How to set up a multi-container data pipeline
- Using Spark for batch ingestion
- Writing dbt models for data transformation
- Orchestrating workflows with Airflow
- Working with Docker in a data engineering context
