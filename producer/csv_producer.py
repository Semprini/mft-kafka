import os
import sys
import csv
import logging
from time import sleep
from json import dumps

from kafka import KafkaProducer

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# from csv_diff import load_csv, compare

logger = logging.getLogger(__name__)


class DummyProducer:
    def __init__(self):
        self.sent = 0

    def produce(self, basename, json_data, flush=False):
        logger.debug(f"Dummy send {basename}: {json_data}")
        self.sent += 1
        return self.sent


class KafkaJsonProducer:
    def __init__(self, topic_prefix, servers=['kafka1:9093', 'kafka2:9094', 'kafka3:9095']):
        self.topic_prefix = topic_prefix
        self.flushes = 0
        self.sent = 0

        self._producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    def produce(self, basename, json_data, flush=False):
        topic_name = self.get_topic_name(basename)
        future_metadata = self._producer.send(topic_name, value=json_data)
        self.sent += 1

        if flush:
            self._producer.flush()
            self.flushes += 1

        return future_metadata

    def get_topic_name(self, basename):
        return self.topic_prefix + basename

    def flush(self):
        self._producer.flush()
        self.flushes += 1

    def stop(self):
        self._producer.flush()
        self._producer.close()


class CSVSource():
    def __init__(self, path, producer, topic_audit, delta_mode=True):
        self.path = path
        self.events = 0
        self.producer = producer
        self.topic_audit = topic_audit
        self.file_stats = {}

        # Setup async file watch for csv file changes
        self.event_handler = PatternMatchingEventHandler(patterns=["*.csv", ],
                                                         ignore_patterns=[],
                                                         ignore_directories=True)
        if os.name == 'nt':
            # TODO: Change to on_closed when watchdog implements on Windows
            self.event_handler.on_modified = self.file_trigger
        else:
            self.event_handler.on_closed = self.file_trigger
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.path, recursive=False)
        self.observer.start()

        logger.info(f"Started watchdog observer for '*.csv' at {self.path}")

    def file_trigger(self, event):
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
        self.producer.stop()

    def debatch(self, file_name, file):
        logger.info(f"CSV Debatching {file_name}")
        basename = os.path.basename(file_name)
        mtime = os.stat(file_name).st_mtime

        dict_reader = csv.DictReader(file)

        row_count = 0
        future_metadata = None
        for row in dict_reader:
            row['modified'] = f"{mtime}"
            future_metadata = self.producer.produce(basename, json_data=row)
            row_count += 1

        if future_metadata is not None:
            # Writes are async so final offset not guaranteed to be populated until flush
            self.producer.flush()
            final_offset = future_metadata.get().offset

            self.audit(file_name, self.producer.get_topic_name(basename), row_count, mtime, final_offset)
            logger.info(f"CSV Sent {file_name} to {self.producer.get_topic_name(basename)}")

    def audit(self, file_name, topic_name, row_count, mtime, final_offset):
        """ TODO: Checksum as we debatch
            TODO: Config aggregations on columns to use in audit
        """
        audit_record = {
            "file_name": f"{file_name}",
            "topic_name": f"{topic_name}",
            "modified": f"{mtime}",
            "row_count": row_count,
            "final_offset": final_offset,
        }
        self.producer.produce(self.topic_audit, json_data=audit_record, flush=True)


class Config:
    def __init__(self):
        self.jobs = {}
        self.servers = ['kafka1:9093', 'kafka2:9094', 'kafka3:9095']
        self.audit_topic = 'csv_audit'

    def from_yaml(self, filename):
        logging.info(f"Parsing config from yaml file {filename}")
        import yaml
        with open(sys.argv[1], 'r') as file:
            config_yml = yaml.safe_load(file)
            self.jobs = config_yml['jobs']
            self.servers = config_yml.get('servers', self.servers)
            self.audit_topic = config_yml.get('audit_topic', self.audit_topic)

    def from_args(self):
        logging.info("Parsing config from cmd arguments")
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("path", help="Path where files will be read from (all *.csv files).")
        parser.add_argument("prefix", help="Prefix for topic name where files will be published", default='csv_')
        parser.add_argument("audit", nargs='?', help="Topic name for audit records. Default: csv_audit", default='csv_audit')
        parser.add_argument("servers", nargs='?', help="List of Kafka servers and ports. Default: kafka1:9093,kafka2:9094,kafka3:9095", default='kafka1:9093,kafka2:9094,kafka3:9095')
        args = parser.parse_args()

        self.jobs['args'] = {}
        self.jobs['args']['path'] = args.path
        self.jobs['args']['topic_prefix'] = args.prefix
        self.audit_topic = args.audit
        self.servers = args.servers.split(',')

    def from_env(self):
        logging.info("Parsing config from environment vars")
        self.jobs['args'] = {}
        self.jobs['args']['path'] = os.environ.get('CSV_IN_PATH')
        self.jobs['args']['topic_prefix'] = os.environ.get('CSV_PREFIX')
        self.audit_topic = os.environ.get('CSV_AUDIT', 'csv_audit')
        self.servers = os.environ.get('CSV_SERVERS', 'kafka1:9093,kafka2:9094,kafka3:9095').split(',')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)-8s | %(threadName)s | %(filename)s | %(lineno)d | %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')
    config = Config()
    sources = []

    if len(sys.argv) == 2:
        with open(sys.argv[1], 'r') as file:
            config.from_yaml(file)
            print(config.__dict__)
    elif len(sys.argv) > 2:
        config.from_args()
    else:
        config.from_env()

    for job, details in config.jobs.items():
        producer = KafkaJsonProducer(details['topic_prefix'], config.servers)
        sources.append(CSVSource(details['path'], producer, config.audit_topic))

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        for source in sources:
            source.stop()
