FROM python:3.10-slim-buster

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY csv_producer.py csv_producer.py

CMD [ "python", "-m" , "csv_producer"]
