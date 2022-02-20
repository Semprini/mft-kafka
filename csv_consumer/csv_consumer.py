import os, sys
import json
from typing import Dict
from kafka import KafkaConsumer


class Consumer:
    def __init__(self, topics):
        self.topics = topics

        self.consumer = KafkaConsumer(
            bootstrap_servers=['kafka1:9093', 'kafka2:9094', 'kafka3:9095'],
            auto_offset_reset='latest', enable_auto_commit=True,
            auto_commit_interval_ms=1000)

        self.consumer.subscribe(topics=[self.topics])


class CSVConsumer(Consumer):
    """ A Kafka Consumer which subscribes to 1 or more CSV file transfer topics plus an audit topic
        Producer will publish an audit record to the audit topic after all data has been published for each modified file time.
        This consumer will write files with source modified file name I.e. <topic name>_<modified stamp>.csv
        TODO: Check topic ordering and make sure all rows are read before audit processed
        TODO: Write .tmp file then rename at audit process
    """

    def __init__(self, topic, topic_audit, path):
        topics = [self.topic_audit, self.topic]
        super().__init__(topics)

        self.topic = topic
        self.topic_audit = topic_audit
        self.path = path
        self.writers = {}

        self.consume()

    def consume(self):
        for message in self.consumer:
            value = json.loads(message.value)
            if message.topic == self.topic_audit:
                # End trigger found, write outstanding data and audit the completed file
                if value['topic_name'] in self.writers.keys():
                    self.writers[value['topic_name']].audit(message.value)
                else:
                    print("Error: Audit entry found for a file which does not exist")
            else:
                # Check if the topic is in the dictionary of writers, create if not
                if not message.topic in self.writers.keys():
                    self.writers[message.topic] = CSVWriter(self.path, message.topic)
                self.writers[message.topic].buffer(message)


class CSVWriter():
    def __init__(self, path, topic):
        self.path = path
        self.topic = topic
        self._buffer = []
        self._buffer_max_len = 100
        self._columns = None
        self._written_count = 0

    def buffer(self, message):
        self._buffer.append( json.loads(message.value) )
        if len(self._buffer) > self._buffer_max_len:
            self.write()
    
    def write(self):
        file_name = self.path + self.topic + "_" + self._buffer[0]['modified'] + ".csv"
        with open( file_name, "w+") as csvfile:
            for row in self._buffer:
                csvfile.write(row)
        self._written_count += len(self._buffer)
        self._buffer = []

    def audit(self, audit_record):
        if len(self._buffer) > 0:
            self.write()
        if self._written_count == audit_record['row_count']:
            print("File complete and written. Honest. Would I lie to you?")
        else:
            print("Error: Incomplete file written with audit record found")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    topic = sys.argv[2] if len(sys.argv) > 2 else 'csv_'
    topic_audit = sys.argv[2] if len(sys.argv) > 2 else 'csvaudit'
    
    trigger = CSVConsumer(topic, topic_audit, path)
