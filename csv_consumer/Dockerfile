FROM python:3.10-slim-buster

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY csv_consumer.py csv_consumer.py

CMD [ "python", "-m" , "csv_consumer"]
