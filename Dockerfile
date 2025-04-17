FROM apache/airflow:2.9.2-python3.10

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --upgrade pip \
 && pip install -r /requirements.txt

# Only change to root AFTER pip installs (not during)
USER root