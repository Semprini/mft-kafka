import unittest
from time import sleep
import sys
sys.path.append("..")

from tests.mocks import stub_KafkaProducer
from producer.csv_producer import JsonProducer, CSVProducer

class TestJsonProducer(unittest.TestCase):
    def setUp(self):
        self.assertTrue(stub_KafkaProducer().foo)

    def test_stub_kafka(self):
        p = JsonProducer()
        self.assertTrue(p.producer.foo)


def on_closed(event):
    print("closed")

class TestCSVProducer(unittest.TestCase):
    def setUp(self):
        with open("./tests/files_in/test.csv", "w") as file:
            data = "column 1, column 2\nvalue 1,value 2"
            file.write(data)

        self.producer = CSVProducer("C:\\Dev\\python\\mft-kafka\\tests\\files_in\\","test_","audit", on_closed)

    def tearDown(self):
        self.producer.stop()

    def test_file_change(self):
        with open("./tests/files_in/test.csv", "w") as file:
            data = "column 1, column 2\nvalue 3,value 4"
            file.write(data)
        sleep(1)
        self.assertEqual(self.producer.events, 1)

if __name__ == '__main__':
    unittest.main()
