#!/usr/bin/env python
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import os
nltk.download('vader_lexicon')

def main():
    # Load .env file into environment
    load_dotenv()
    sid = SentimentIntensityAnalyzer()

    # Read in environment variables to configure setup
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM')
    security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL')
    sasl_plain_username = os.getenv('KAFKA_API_KEY')
    sasl_plain_password = os.getenv('KAFKA_API_SECRET')
    consumer_group_id = os.getenv('KAFKA_CONSUMER_GROUP_ID')
    consumer_topic = os.getenv('KAFKA_CONSUMER_TOPIC')
    producer_group_id = os.getenv('KAFKA_PRODUCER_GROUP_ID')
    producer_topic = os.getenv('KAFKA_PRODUCER_TOPIC')
    auto_offset_reset = 'earliest'

    # Create a Kafka consumer, a client to read in requests from the cluster
    consumer = KafkaConsumer(
        consumer_topic,
        bootstrap_servers=bootstrap_servers,
        sasl_mechanism=sasl_mechanism,
        security_protocol=security_protocol,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        group_id=consumer_group_id,
        auto_offset_reset=auto_offset_reset,
    )

    # Create a Kafka producer, a client to send in responses to the cluster
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        sasl_mechanism=sasl_mechanism,
        security_protocol=security_protocol,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
    )

    # Process messages
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        #print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
        #                                     message.offset, message.key,
        #                                     message.value))
        polarity_scores = sid.polarity_scores(json.loads(message.value.decode('utf-8'))["value0"])
        print(polarity_scores)
        producer.send(producer_topic, json.dumps(polarity_scores, ensure_ascii=False).encode('utf8'))

if __name__ == '__main__':
    main()
