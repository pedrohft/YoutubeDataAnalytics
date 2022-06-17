import pytchat
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

producer = KafkaProducer(bootstrap_servers="localhost:29092")

chat = pytchat.create(video_id="5qap5aO4i9A")

while chat.is_alive():
#  for c in chat.get().sync_items():
#     s = "".join(f"{c.datetime} [{c.author.name}]- {c.message}")
    for c in chat.get().items:
        print(c.json())
        producer.send("youtube", json.dumps(c.json()).encode('utf-8'))
        time.sleep(5)
    # print(f"{c.datetime} [{c.author.name}]- {c.message}")


producer.flush()