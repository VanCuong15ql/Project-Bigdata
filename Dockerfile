FROM apache/airflow:2.6.0-python3.9

USER root
RUN apt-get update && apt-get install -y \
    chromium-driver \
    && apt-get clean

# Trở lại user airflow để cài requirements
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt