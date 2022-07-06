import pytchat
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import json
import logging


class Producer():
    """
    Messages producer writing json entries from Youtbe/Twitch to a Kafka Stream
    """

    def __init__(self, bootstrap_servers, topic, video_id):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.chat = pytchat.create(video_id=video_id)

    def get_messages(self):

        try:
            logging.info("Starting the process of send messages...")
            while self.chat.is_alive():
                for c in self.chat.get().items:
                    # print(c.json())
                    future = self.producer.send("youtube",json.dumps(c.json()).encode('utf-8'))\
                                                                    .add_callback(self.on_send_success)\
                                                                        .add_errback(self.on_send_error)

                assert future.succeeded
            self.producer.flush()
            logging.info("Process finished...")

        except KafkaTimeoutError as kte:
            logging.exception("KafkaProducer timeout sending json to Kafka: %s", kte)
            raise Exception("KafkaProducer timeout sending json to Kafka: %s" % kte)
        except KafkaError as ke:
            logging.exception("KafkaProducer error sending json to Kafka: %s", ke)
            raise Exception("KafkaProducer error sending json to Kafka: %s" % ke)
        except Exception as e:
            logging.exception("KafkaProducer exception sending json to Kafka: %s", e)
            raise Exception("KafkaProducer exception sending json to Kafka: %s" % e)


    def on_send_success(self, event):
         logging.info(f"Messages successful sended: {event}")

    def on_send_error(self, event):
        logging.error(f"Error when try to send messages: {event}")


producer = Producer(bootstrap_servers="localhost:29092", topic="youtube", video_id="5qap5aO4i9A")
producer.get_messages()

print("FINALIZOU")