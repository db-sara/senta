#!/usr/bin/env python
import json
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import os


def main():
    load_dotenv()

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        sasl_mechanism=os.getenv('KAFKA_SASL_MECHANISM'),
        security_protocol=os.getenv('KAFKA_SECURITY_PROTOCOL'),
        sasl_plain_username=os.getenv('KAFKA_API_KEY'),
        sasl_plain_password=os.getenv('KAFKA_API_SECRET'),
        # group_id=os.getenv('KAFKA_BOOTSTRAP_SERVER'),
        auto_offset_reset='earliest',
    )
    # Subscribe to topic
    consumer.subscribe(['nlp-requests'])

    # Process messages
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
