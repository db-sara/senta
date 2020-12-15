#!/usr/bin/env python
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import json
from kafka import KafkaConsumer, KafkaProducer
from string import punctuation
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
nltk.download('vader_lexicon')
nltk.download('punkt')
nltk.download('stopwords')

def summarize(text):
    #tokenize sentences
    sentences = nltk.sent_tokenize(text)

    #set stop words
    stops = list(set(stopwords.words('english'))) + list(punctuation)

    #vectorize sentences and remove stop words
    vectorizer = TfidfVectorizer(stop_words = stops)

    #transform using TFIDF vectorizer
    trsfm=vectorizer.fit_transform(sentences)

    #creat df for input article
    text_df = pd.DataFrame(trsfm.toarray(),columns=vectorizer.get_feature_names(),index=sentences)

    #declare how many sentences to use in summary
    num_sentences = int(np.ceil(text_df.shape[0]**.5))

    #find cosine similarity for all sentence pairs
    similarities = cosine_similarity(trsfm, trsfm)

    #create list to hold avg cosine similarities for each sentence
    avgs = []
    for i in similarities:
        avgs.append(i.mean())

    #find index values of the sentences with highest cosine similarities
    indexes = sorted(np.argsort(avgs)[-num_sentences:])

    #extract the selected sentences from the original text
    sent_list = nltk.sent_tokenize(text)
    sents = []
    for i in indexes:
        sents.append(sent_list[i].replace('\n', ''))

    #join sentences together for final summary
    summary = ' '.join(sents)
    return summary

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
        request = json.loads(message.value.decode('utf-8'))
        content_type = request["type"]
        body = request["body"]
        polarity_scores = sid.polarity_scores(body)
        if content_type == "news":
            summary = summarize(body)
            summary_scores = sid.polarity_scores(summary)
            producer.send(producer_topic, key = message.key, value = json.dumps({'type': content_type, 'overall_sentiment': polarity_scores['compound'], 'summary': summary, 'summary_sentiment': summary_scores['compound']}, ensure_ascii=False).encode('utf8'))
        else:
            producer.send(producer_topic, key = message.key, value = json.dumps({'type': content_type, 'overall_sentiment': polarity_scores['compound']}, ensure_ascii=False).encode('utf8'))        
if __name__ == '__main__':
    main()
