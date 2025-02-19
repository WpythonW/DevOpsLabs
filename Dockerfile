FROM apache/airflow:2.7.1

RUN pip install --no-cache-dir apache-airflow-providers-postgres

WORKDIR /opt/airflow
COPY dags/ /opt/airflow/dags/