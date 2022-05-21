import os
import sys
import json
import logging

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class AuditException(Exception):
    pass


class KafkaJsonConsumer:
    """ A Kafka Consumer which subscribes to 1 or more CSV file transfer topics plus an audit topic
        Producer will publish an audit record to the audit topic after all data has been published for each modified file time.
        This consumer will write files with source modified file name I.e. <topic name>_<modified stamp>.csv
        TODO: Check topic ordering and make sure all rows are read before audit processed
    """
    def __init__(self, writer, topic, topic_audit="csv_audit", servers=['kafka1:9093', 'kafka2:9094', 'kafka3:9095']):
        self.topic = topic
        self.topic_audit = topic_audit
        self.writer = writer
        self.writers = {}
        self.audits = {}
        self.offsets = {}

        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            auto_offset_reset='latest', enable_auto_commit=True,
            auto_commit_interval_ms=1000)

        # TODO: Move to current offset

        topics = [self.topic_audit, self.topic]
        self.consumer.subscribe(topics=[topics])
        self.consume()

    def consume(self):
        for message in self.consumer:
            self.offsets[message.topic] = message.offset
            if message.topic == self.topic_audit:
                self.process_audit_record(message)
            else:
                self.process_record(message)

    def process_record(self, message):
        # Check if the topic is in the dictionary of writers, create if not
        if message.topic not in self.writers.keys():
            self.writers[message.topic] = self.writer(self.path, message.topic)
        self.writers[message.topic].buffer(message)

        if message.topic in self.audits.keys() and message.offset == self.audits[message.topic]:
            self.finalise(message.topic)

    def process_audit_record(self, message):
        value = json.loads(message.value)
        topic_name = value['topic_name']
        self.audits[topic_name] = value

        if topic_name in self.writers.keys() and self.offsets[topic_name] == value['final_offset']:
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
        self.tmp_file_name = self.path + "tmp" + self.topic + "_" + self._buffer[0]['modified'] + ".csv"
        self.err_file_name = self.path + "err" + self.topic + "_" + self._buffer[0]['modified'] + ".csv"
        self.file_name = self.path + self.topic + "_" + self._buffer[0]['modified'] + ".csv"

    def buffer(self, message):
        self._buffer.append(json.loads(message.value))
        if len(self._buffer) > self._buffer_max_len:
            self.write()

    def write(self):
        with open(self.tmp_file_name, "w+") as csvfile:
            for row in self._buffer:
                csvfile.write(row)
        self._written_count += len(self._buffer)
        self._buffer = []

    def finalise_file(self, audit_record):
        if len(self._buffer) > 0:
            self.write()
        if self._written_count == audit_record['row_count']:
            os.replace(self.tmp_file_name, self.file_name)
            logger.info("File complete and written. Honest. Would I lie to you?")
        else:
            os.replace(self.tmp_file_name, self.err_file_name)
            logger.error("Error: Written file does not match audit record.")
            raise AuditException("Write count missmatch with audit record")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)-8s | %(threadName)s | %(filename)s | %(lineno)d | %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')

    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    topic = sys.argv[2] if len(sys.argv) > 2 else 'csv_test'
    topic_audit = sys.argv[2] if len(sys.argv) > 2 else 'csv_audit'

    trigger = KafkaJsonConsumer(CSVWriter, topic, topic_audit)
