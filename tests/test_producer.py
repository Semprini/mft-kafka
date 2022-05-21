import unittest
from time import sleep

from tests.mocks import stub_KafkaProducer
from producer.csv_producer import KafkaJsonProducer, CSVSource, DummyProducer


class TestJsonProducer(unittest.TestCase):
    def setUp(self):
        self.assertTrue(stub_KafkaProducer().foo)

    def test_stub_kafka(self):
        p = KafkaJsonProducer("")
        self.assertTrue(p.producer.foo)


def on_closed(event):
    print("closed")


class TestCSVProducer(unittest.TestCase):
    def setUp(self):
        with open("./tests/files_in/test.csv", "w") as file:
            data = "column 1, column 2\nvalue 1,value 2"
            file.write(data)

        producer = DummyProducer()
        self.source = CSVSource("C:\\Dev\\python\\mft-kafka\\tests\\files_in\\", producer, "audit")

    def tearDown(self):
        self.source.stop()

    def test_file_change(self):
        with open("./tests/files_in/test.csv", "w") as file:
            data = "column 1, column 2\nvalue 3,value 4"
            file.write(data)
        sleep(1)
        self.assertEqual(self.source.events, 1)
