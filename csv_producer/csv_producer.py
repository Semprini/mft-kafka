from json import dumps
from kafka import KafkaProducer

import csv
import os
import sys
from time import sleep

from watchdog.observers import Observer
import watchdog.events


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka1:9093', 'kafka2:9094', 'kafka3:9095'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )


class CSVProducer(Producer):
    def __init__(self, path, topic_prefix, topic_audit):
        super().__init__()
        self.path = path
        self.topic_prefix = topic_prefix
        self.topic_audit = topic_audit

        # Setup async file watch for csv file closes
        self.event_handler = watchdog.events.PatternMatchingEventHandler(patterns=["*.csv", ],
                                    ignore_patterns=[],
                                    ignore_directories=True)
        self.event_handler.on_closed = self.on_closed
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.path, recursive=False)
        self.observer.start()

    def on_closed(self, event):
        self.produce(event.src_path)

    def stop(self):
        self.observer.stop()
        self.observer.join()

    def produce(self, file_name):
        topic_name = self.topic_prefix + os.path.basename(file_name)

        mtime = os.stat(file_name).st_mtime

        with open(file_name, "r") as csvfile:
            dict_reader = csv.DictReader(csvfile)

            row_count = 0
            for row in dict_reader:
                row['modified'] = f"{mtime}"
                self.producer.send(topic_name, value=row)
                row_count += 1

            if row_count > 0:
                self.audit( file_name, topic_name, row_count, mtime )
                print(f"sent {file_name} to {topic_name}")

    def audit(self, file_name, topic_name, row_count, mtime):
        audit_record = {
            "file_name": f"{file_name}",
            "topic_name": f"{topic_name}",
            "modified": f"{mtime}",
            "row_count": f"{row_count}",
        }
        self.producer.send(self.topic_audit, value=audit_record)
        self.producer.flush()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    topic_prefix = sys.argv[2] if len(sys.argv) > 2 else 'csv_'
    topic_audit = sys.argv[2] if len(sys.argv) > 2 else 'csvaudit'
    producer = CSVProducer(path, topic_prefix, topic_audit)
    
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        producer.stop()

