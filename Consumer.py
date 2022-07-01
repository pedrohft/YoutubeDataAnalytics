from kafka import KafkaConsumer
import pandas as pd
import json
import logging as log
from data.utils import forgiving_json_deserializer


class Consumer():
    def __init__(self, bootstrap_server, topic, auto_offset_reset):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.auto_offset_reset = auto_offset_reset
        self.consumer = KafkaConsumer(self.topic,bootstrap_servers=self.bootstrap_server, 
                                            value_deserializer = forgiving_json_deserializer,
                                            auto_offset_reset = self.auto_offset_reset)
        self.df = pd.DataFrame()


    def consume_messages(self):
        # df = pd.DataFrame(columns=["datetime", "name", "message"])

        for msg in self.consumer:
            msg = json.loads(msg.value)

            #colocar na parte bash
            # with open('json_data.json', 'w') as outfile:
            #     json.dump(msg, outfile)

            #Colocar na parte stream esse frame

            self.load_dataframe_to_mysql(msg)
            # print(df.count())
            
    def load_dataframe_to_mysql(self, msg):
            self.df = pd.concat([self.df,pd.DataFrame({
                "id": [msg["id"]],
                "datetime": [pd.to_datetime(msg["datetime"])],
                "name": [msg["author"]["name"]],
                "isVerified": [msg["author"]["isVerified"]],
                "isChatOwner": [msg["author"]["isChatOwner"]],
                "isChatSponsor": [msg["author"]["isChatSponsor"]],
                "isChatModerator": [msg["author"]["isChatModerator"]],
                "channelId": [msg["author"]["channelId"]],
                "channelUrl": [msg["author"]["channelUrl"]],
                "message":  [msg["message"]],
                "amountValue": [msg["amountValue"]],
                "amountString": [msg["amountString"]],
                "fromSource": ["Youtube"]
            })],ignore_index=True) 

            print(self.df.count())

    def save_json_to_data_lake(msg):
        pass


consumer = Consumer(bootstrap_server="localhost:29092", topic="youtube", auto_offset_reset="earliest")
consumer.consume_messages()