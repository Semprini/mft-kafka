import sys
import os
import json
import logging
import csv

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class AuditException(Exception):
    pass


class KafkaJsonConsumer:
    """ A Kafka Consumer which subscribes to 1 or more CSV file transfer topics plus an audit topic
        Producer will publish an audit record to the audit topic after all data has been published for each modified file time.
        This consumer will write files with source modified file name I.e. <topic name>_<modified stamp>.csv
    """
    def __init__(self, writer, path: str, topics: list, topic_audit: str = "csv_audit", servers: list = ['kafka1:9093', 'kafka2:9094', 'kafka3:9095']):
        self.topics: list = topics
        self.topic_audit: str = topic_audit
        self.writer = writer
        self.path: str = path
        self.writers: dict = {}
        self.audits: dict = {}
        self.offsets: dict = {}

        self.consumer = KafkaConsumer(bootstrap_servers=servers,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      # group_id='csv-consumer-group',
                                      auto_commit_interval_ms=2000)

        subscribe_topics = topics
        subscribe_topics.append(self.topic_audit)
        self.consumer.subscribe(topics=subscribe_topics)

    def consume(self):
        for message in self.consumer:
            self.offsets[message.topic] = message.offset
            if message.topic == self.topic_audit:
                logging.debug("Processing audit topic message")
                self.process_audit_record(message)
            else:
                logging.debug("Processing data topic message")
                self.process_record(message)

    def process_record(self, message):
        # Check if the topic is in the dictionary of writers, create if not
        if message.topic not in self.writers.keys():
            logging.info(f"New file beginning: {message.topic}")
            self.writers[message.topic] = self.writer(self.path, message.topic)
        self.writers[message.topic].buffer(message)

        if message.topic in self.audits.keys() and message.offset == self.audits[message.topic]['final_offset']:
            logging.info(f"Finalising on last message receieved: {message.topic}")
            self.finalise(message.topic)

    def process_audit_record(self, message):
        value = json.loads(message.value)
        topic_name = value['topic_name']
        self.audits[topic_name] = value

        if topic_name in self.writers.keys() and self.offsets[topic_name] == value['final_offset']:
            logging.info(f"Finalising on audit received: {topic_name}")
            self.finalise(topic_name)

    def finalise(self, topic_name):
        # All rows written and audit record exists, write outstanding data and audit the completed file
        self.writers[topic_name].finalise_file(self.audits[topic_name])
        del self.writers[topic_name]
        del self.audits[topic_name]
        del self.offsets[topic_name]

    def stop(self):
        self.consumer.close()


class CSVWriter():
    def __init__(self, path, topic):
        self.path = path
        self.topic = topic
        self._buffer = []
        self._buffer_max_len = 100
        self._columns = None
        self._written_count = 0

        self.tmp_file_name = None
        self.err_file_name = None
        self.file_name = None

    def buffer(self, message):
        logging.debug("Buffering")
        data = json.loads(message.value)
        self._buffer.append(data)
        if len(self._buffer) > self._buffer_max_len:
            self.write()

    def write(self):
        header_row = False
        if self.tmp_file_name is None:
            self.tmp_file_name = self.path + "tmp" + self.topic + "_" + self._buffer[0]['modified'] + ".csv"
            self.err_file_name = self.path + "err" + self.topic + "_" + self._buffer[0]['modified'] + ".csv"
            self.file_name = self.path + self.topic + "_" + self._buffer[0]['modified'] + ".csv"
            header_row = True

        with open(self.tmp_file_name, "w+", newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            if header_row:
                csvwriter.writerow(list(self._buffer[0].keys())[:-1])
            for row in self._buffer:
                del row['modified']
                csvwriter.writerow(row.values())

        self._written_count += len(self._buffer)
        self._buffer = []

    def finalise_file(self, audit_record):
        if len(self._buffer) > 0:
            self.write()
        if self._written_count == audit_record['row_count']:
            os.replace(self.tmp_file_name, self.file_name)
            logger.info(f"File complete and written: {self.file_name}")
        else:
            os.replace(self.tmp_file_name, self.err_file_name)
            logger.error(f"Error: Written file does not match audit record: {self.file_name}")
            # raise AuditException("Write count missmatch with audit record")
        self.tmp_file_name = None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)-8s | %(threadName)s | %(filename)s | %(lineno)d | %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')

    if len(sys.argv) > 1:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("path", help="Path where files will be written.")
        parser.add_argument("topics", help="Topic name or list of topics to subscribe to")
        parser.add_argument("audit", nargs='?', help="Topic name for audit records. Default: csv_audit", default='csv_audit')
        parser.add_argument("servers", nargs='?', help="List of Kafka servers and ports. Default: kafka1:9093,kafka2:9094,kafka3:9095", default='kafka1:9093,kafka2:9094,kafka3:9095')
        args = parser.parse_args()
        path = args.path
        topics = args.topics.split(',')
        audit = args.audit
        servers = args.servers.split(',')
    else:
        path = os.environ.get('CSV_OUT_PATH')
        topics = os.environ.get('CSV_TOPICS')
        topics = topics.split(',')
        audit = os.environ.get('CSV_AUDIT', 'csv_audit')
        servers = os.environ.get('CSV_SERVERS', 'kafka1:9093,kafka2:9094,kafka3:9095').split(',')

    consumer = KafkaJsonConsumer(CSVWriter, path, topics, audit, servers=servers)

    try:
        consumer.consume()
    except KeyboardInterrupt:
        consumer.stop()
