# mft-kafka
File Transfer utility using Kafka as transport. Yay! an inefficient mechanism for copying files! Why?: https://semprini.me/the-forgotton-question-mark/

De-batches to a stream to enable multiple consumers.

Both producer and consumer accept either arguments or falls back to environment variables if no arguments are provided.

## Testing
A docker-compose.yml is provided which assumes a running Kafka. A Docker compose file for Kafka can be found here: https://github.com/Semprini/semprini-blog-pipeline/blob/main/docker-compose.yml (note: the network will pick up the directory name from where it is run use "docker network ls" and verify it against the network in the docker-compose.yml)

 ```
mkdir /tmp/in/
mkdir /tmp/out/
cd tests
docker-compose up -d
```
Any .csv files placed in /tmp/in/ will be debatched and sent to Kafka but only test.csv will be recombined/audited into /tmp/out/

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
