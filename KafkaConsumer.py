from ensurepip import bootstrap
from kafka import KafkaConsumer
import csv

consumer  = KafkaConsumer("youtube",
            bootstrap_servers="localhost:29092")

csvfile = open('results.csv', 'a')

for msg in consumer:
    csvfile.write(str(msg.value) + ",")
    print(msg.value)

# print(consumer.poll())