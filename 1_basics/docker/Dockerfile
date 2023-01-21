FROM python:3.9

WORKDIR /app

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]