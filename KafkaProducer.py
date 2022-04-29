from ensurepip import bootstrap
import pytchat
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:29092")

chat = pytchat.create(video_id="uIx8l2xlYVY")

while chat.is_alive():
 for c in chat.get().sync_items():
    s = "".join(f"{c.datetime} [{c.author.name}]- {c.message}")
    producer.send("youtube", str.encode(s)

     )
    print(f"{c.datetime} [{c.author.name}]- {c.message}")


producer.flush()