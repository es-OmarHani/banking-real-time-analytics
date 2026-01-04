FROM apache/airflow:3.1.5-python3.12

USER airflow

ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY requirements.txt /requirements.txt

# FAB provider for username/password auth + airflow users create
RUN pip install --no-cache-dir apache-airflow-providers-fab --constraint "${CONSTRAINT_URL}" && \
    pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
