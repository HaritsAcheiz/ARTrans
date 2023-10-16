FROM apache/airflow:latest-python3.11
USER root
COPY dags /opt/airflow/dags
COPY config /opt/airflow/config
COPY logs /opt/airflow/logs
COPY plugins /opt/airflow/plugins
RUN chmod 777 /opt/airflow
RUN apt-get update
USER airflow
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /opt/airflow/dags/includes/requirements.txt
RUN export GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/dags/includes/gcp/gcp-service-account.json"