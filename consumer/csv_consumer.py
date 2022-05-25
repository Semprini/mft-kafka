import os
import sys
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
        TODO: Check topic ordering and make sure all rows are read before audit processed
    """
    def __init__(self, writer, path, topic, topic_audit="csv_audit", servers=['kafka1:9093', 'kafka2:9094', 'kafka3:9095']):
        self.topic = topic
        self.topic_audit = topic_audit
        self.writer = writer
        self.path = path
        self.writers = {}
        self.audits = {}
        self.offsets = {}

        self.consumer = KafkaConsumer(bootstrap_servers=servers,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      auto_commit_interval_ms=2000)
        # group_id='csv-consumer-group')

        topics = [self.topic_audit, self.topic]
        self.consumer.subscribe(topics=topics)

    def consume(self):
        for message in self.consumer:
            self.offsets[message.topic] = message.offset
            if message.topic == self.topic_audit:
                logging.info("Processing audit topic message")
                self.process_audit_record(message)
            else:
                logging.info("Processing data topic message")
                self.process_record(message)

    def process_record(self, message):
        # Check if the topic is in the dictionary of writers, create if not
        if message.topic not in self.writers.keys():
            self.writers[message.topic] = self.writer(self.path, message.topic)
        self.writers[message.topic].buffer(message)

        if message.topic in self.audits.keys():
            print(f"DATA offset:{message.offset}")
            print(f"DATA Final Offset:{self.audits[message.topic]['final_offset']}")
            print(f"DATA Writers:{self.writers.keys()}")
            print(message.offset == self.audits[message.topic]['final_offset'])
            print(type(message.offset))
            print(type(self.audits[message.topic]['final_offset']))
            print(self.audits[message.topic])

        if message.topic in self.audits.keys() and message.offset == self.audits[message.topic]['final_offset']:
            logging.info("Finalising on last message receieved")
            self.finalise(message.topic)

    def process_audit_record(self, message):
        value = json.loads(message.value)
        topic_name = value['topic_name']
        self.audits[topic_name] = value

        if topic_name in self.offsets.keys():
            print(f"AUDIT Offsets:{self.offsets[topic_name]}")
            print(f"AUDIT Final Offset:{value['final_offset']}")
            print(f"AUDIT Writers:{self.writers.keys()}")
            print(f"AUDIT:{value}")
            print(self.offsets[topic_name] == value['final_offset'])
            print(type(self.offsets[topic_name]))
            print(type(value['final_offset']))
            print(value)

        if topic_name in self.writers.keys() and self.offsets[topic_name] == value['final_offset']:
            logging.info("Finalising on audit received")
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
        logging.info("Buffering")
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

        with open(self.tmp_file_name, "w+", newline='' ) as csvfile:
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
            logger.info("File complete and written. Honest. Would I lie to you?")
        else:
            os.replace(self.tmp_file_name, self.err_file_name)
            logger.error("Error: Written file does not match audit record.")
            raise AuditException("Write count missmatch with audit record")
        self.tmp_file_name = None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)-8s | %(threadName)s | %(filename)s | %(lineno)d | %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')

    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    topic = sys.argv[2] if len(sys.argv) > 2 else 'csv_test.csv'
    topic_audit = sys.argv[2] if len(sys.argv) > 2 else 'csv_audit'
    consumer = KafkaJsonConsumer(CSVWriter, path, topic, topic_audit, servers=['192.168.1.71:9093', '192.168.1.71:9094', '192.168.1.71:9095'])

    try:
        consumer.consume()
    except KeyboardInterrupt:
        consumer.stop()
