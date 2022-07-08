from asyncio.log import logger
from itertools import count
from kafka import KafkaConsumer
import pandas as pd
import json
import logging as log
from data.utils import forgiving_json_deserializer
import os

class Consumer():
    def __init__(self,channel, source, bootstrap_server, topic, auto_offset_reset):
        self.source = source
        self.channel = channel
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.auto_offset_reset = auto_offset_reset
        self.consumer = KafkaConsumer(self.topic,bootstrap_servers=self.bootstrap_server, 
                                            value_deserializer = forgiving_json_deserializer,
                                            auto_offset_reset = self.auto_offset_reset)
        self.df = pd.DataFrame()


    def consume_messages(self):

        self.create_folder()
        counter = 0

        for msg in self.consumer:
            msg = json.loads(msg.value)

            self.load_dataframe(msg)
            self.save_json_to_data_lake(msg, counter)
            
            counter += 1
            
    def load_dataframe(self, msg):
            self.df = pd.concat([self.df,pd.DataFrame({
                "channel" : [[self.channel]],
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
                "fromSource": [self.source]
            })],ignore_index=True) 

            print(self.df.count())

    def save_json_to_data_lake(self, msg, counter):
        
            with open(f'.\\datalake\\bronze\\{self.channel}\\{counter}.json', 'a') as outfile:
                
                json.dump(msg, outfile, indent=6)
                outfile.close
            
            log.info("JSON saved")

    def create_folder(self):
        if os.path.exists(f"{os.getcwd()}\\datalake\\bronze\\{self.channel}") :
            log.info("Folder already exisits")
        else:
            os.makedirs(f"{os.getcwd()}\\datalake\\bronze\\{self.channel}")

# consumer = Consumer("lofi-hiphop","youtube",bootstrap_server="localhost:29092", topic="youtube", auto_offset_reset="earliest")
# consumer.consume_messages()


consumer = Consumer("cerol","youtube",bootstrap_server="localhost:29092", topic="youtube", auto_offset_reset="earliest")
consumer.consume_messages()
