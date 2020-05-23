import os
import argparse
from kafka import KafkaProducer
import fileinput
import time

def sendDataToKafkaServer():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", help="path of data file")
    parser.add_argument("--host", help="Kafka Server's host")
    parser.add_argument("--port", help="Kafka Server's port")

    args = parser.parse_args()
    host = args.host
    port = args.port
    data_path = args.data_path
    
    topic = "test"
    producer = KafkaProducer(bootstrap_servers= host + ":" + port)
    
    fr = open(data_path, 'r')
    data = fr.read()
    all_batch = data.split("end\n")
    for batch in all_batch:
        if str(batch).strip() != "":     
            producer.send(topic, bytes(batch))
            print(batch)
            time.sleep(5)
    fr.close()


if __name__ == "__main__":
    sendDataToKafkaServer()

