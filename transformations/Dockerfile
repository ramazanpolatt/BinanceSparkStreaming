FROM python:3.10-slim

# Install necessary packages
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Dbt-trino kurulumu
RUN pip install --no-cache-dir dbt-trino

#Install postgres connector
RUN pip install dbt-postgres

# Working directory
WORKDIR /usr/app/dbt

#To Keep container working
ENTRYPOINT ["sleep", "infinity"]