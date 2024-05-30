from kafka import KafkaProducer
from pymongo import MongoClient
import time
import os 
import json
from pprint import pprint

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'twitter'
uri = "mongodb+srv://nvkhoa14:UITHKT@cluster0.irat42q.mongodb.net"

client = MongoClient(uri)
client.list_database_names()

coll = client.lab4.chatgpt_tweets
data = coll.find()

def error_callback(exc):
    raise Exception('Error while sendig data to kafka: {0}'.format(str(exc)))

def serializer(message):
    return json.dumps(message).encode('utf-8')

def write_to_kafka(topic_name, items):
    count = 0
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
    for message in items:
        producer.send(topic_name, serializer({'message': message['Tweet']})).add_errback(error_callback)
        count+=1
        print("\rWrote {0} messages into topic: {1}         ".format(count, topic_name), end='')
        # time.sleep(0.5)
    producer.flush()
    
write_to_kafka("tweet", data)