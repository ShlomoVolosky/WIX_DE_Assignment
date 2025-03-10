FROM python:3.10-slim

# Set airflow env variables
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_VERSION=2.5.2
ENV PYTHONUNBUFFERED=1

WORKDIR $AIRFLOW_HOME

# Install system deps
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    nano \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install airflow (and optional extras like postgres, etc.)
RUN pip install --no-cache-dir \
    apache-airflow==${AIRFLOW_VERSION} \
    apache-airflow-providers-postgres \
    requests \
    pandas \
    sqlalchemy

# Copy your pipeline code (if you want it inside this image),
# or you can mount it as a volume in docker-compose.
COPY ./main_dag.py $AIRFLOW_HOME/dags/

# If you want to copy pipeline code as well:
# COPY ../src/ $AIRFLOW_HOME/dags/src/

# Expose port for webserver
EXPOSE 8080

# We’ll define the entrypoint/cmd in docker-compose for easier override
CMD ["airflow", "webserver"]

