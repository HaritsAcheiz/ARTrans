FROM apache/airflow:latest-python3.11
USER root
COPY includes /opt/airflow/dags
COPY requirements.txt /
RUN apt-get update
USER airflow
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
