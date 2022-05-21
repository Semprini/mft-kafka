import os
import sys
import csv
import logging
from time import sleep
from json import dumps

from kafka import KafkaProducer

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

logger = logging.getLogger(__name__)


class DummyProducer:
    def __init__(self):
        self.sent = 0

    def produce(self, basename, json_data, flush=False):
        logger.debug(f"Dummy send {basename}: {json_data}")
        self.sent += 1
        return self.sent


class KafkaJsonProducer:
    def __init__(self, topic_prefix, servers=['192.168.1.71:9093', '192.168.1.71:9094', '192.168.1.71:9095']):  # ['kafka1:9093', 'kafka2:9094', 'kafka3:9095']):
        self.topic_prefix = topic_prefix
        self.flushes = 0
        self.sent = 0

        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    def produce(self, basename, json_data, flush=False):
        topic_name = self.topic_prefix + os.path.basename(basename)
        metadata = self.producer.send(topic_name, value=json_data)
        self.sent += 1

        if flush:
            self.producer.flush()
            self.flushes += 1

        return metadata.offset

    def stop(self):
        self.producer.flush()
        self.producer.close()


class CSVSource():
    def __init__(self, path, producer, topic_audit):
        self.path = path
        self.events = 0
        self.producer = producer
        self.topic_audit = topic_audit
        self.file_stats = {}

        # Setup async file watch for csv file changes
        self.event_handler = PatternMatchingEventHandler(patterns=["*.csv", ],
                                                         ignore_patterns=[],
                                                         ignore_directories=True)
        self.event_handler.on_modified = self.on_modified
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.path, recursive=False)
        self.observer.start()

        logger.info(f"Started {self.observer} for {self.event_handler} at {self.path}")

    def on_modified(self, event):
        """ TODO: Change to on_closed when watchdog implements on Windows"""
        stat = os.stat(event.src_path)

        # Check that the file has not already been sent by checking it's modified time
        if event.src_path not in self.file_stats.keys() or self.file_stats[event.src_path].st_mtime != stat.st_mtime:
            self.file_stats[event.src_path] = stat

            # If the source system has the file open then we will fail to open and *assume* will get more modified events
            try:
                with open(event.src_path, "r") as file:
                    self.debatch(event.src_path, file)
                    self.events += 1
            except IOError:
                logger.warn(f"File {event.src_path} is not readable")

    def stop(self):
        self.observer.stop()
        self.observer.join()
        logger.info(f"File watcher closed for {self.path}")

    def debatch(self, file_name, file):
        logger.info(f"CSV Debatching {file_name}")
        basename = os.path.basename(file_name)
        mtime = os.stat(file_name).st_mtime

        dict_reader = csv.DictReader(file)

        row_count = 0
        offset = 0
        for row in dict_reader:
            row['modified'] = f"{mtime}"
            offset = self.producer.produce(basename[:-4], json_data=row)
            row_count += 1

        if row_count > 0:
            self.audit(file_name, basename, row_count, mtime, offset)
            logger.info(f"CSV Sent {file_name} to {basename}")

    def audit(self, file_name, topic_name, row_count, mtime, final_offset):
        """ TODO: Checksum as we debatch
            TODO: Config aggregations on columns to use in audit
        """
        audit_record = {
            "file_name": f"{file_name}",
            "topic_name": f"{topic_name}",
            "modified": f"{mtime}",
            "row_count": f"{row_count}",
            "final_offset": f"{final_offset}",
        }
        self.producer.produce(self.topic_audit, json_data=audit_record, flush=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)-8s | %(threadName)s | %(filename)s | %(lineno)d | %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')

    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    topic_prefix = sys.argv[2] if len(sys.argv) > 2 else 'csv_'
    topic_audit = sys.argv[3] if len(sys.argv) > 3 else 'audit'
    producer = KafkaJsonProducer(topic_prefix)  # DummyProducer()
    source = CSVSource(path, producer, topic_audit)

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        source.stop()
        producer.stop()
