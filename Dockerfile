FROM apache/airflow:2.9.1

RUN pip install --no-cache-dir pandas pydantic-settings psycopg2-binary