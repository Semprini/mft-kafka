import kafka


class stub_KafkaProducer:
    def __init__(self, bootstrap_servers=None, value_serializer=None):
        self.foo = True

    def send(self, topic_audit, value):
        pass

    def flush(self):
        print("flush")


kafka.KafkaProducer = stub_KafkaProducer
