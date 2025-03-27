FROM vietlam2002/kafka_spark-airflow-webserver:1.0

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip
RUN pip install  -r /requirements.txt