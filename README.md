# mft-kafka
File Transfer utility using Kafka as transport. De-batches to a stream to enable multiple consumers.

Both producer and consumer accept either arguments or falls back to environment variables if no arguments are provided.

## Producer:
usage: csv_producer.py [-h] path prefix [audit] [servers]

positional arguments:

  - path:        Path where files will be read from (all *.csv files).
  - prefix:      Prefix for topic name where files will be published
  - audit:       Topic name for audit records. Default: csv_audit
  - servers:     List of Kafka servers and ports. Default: kafka1:9093,kafka2:9094,kafka3:9095

> python .\producer\csv_producer.py ".\tests\files_in\" csv_

## Consumer:
usage: csv_consumer.py [-h] path topics [audit] [servers]

positional arguments:

  - path:        Path where files will be written.
  - topics:      Topic name or list of topics to subscribe to
  - audit:       Topic name for audit records
  - servers:     List of Kafka servers and ports. Default: kafka1:9093,kafka2:9094,kafka3:9095

> python .\consumer\csv_consumer.py ".\tests\files_out\" csv_test.csv
