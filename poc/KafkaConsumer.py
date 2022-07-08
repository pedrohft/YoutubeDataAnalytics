from kafka import KafkaConsumer
import pandas as pd
import json
import logging as log

def forgiving_json_deserializer(v):
    if v is None:
        return
    try:
        return json.loads(v.decode('utf-8'))
    except json.decoder.JSONDecodeError:
        log.exception('Unable to decode: %s', v)
        return None

consumer  = KafkaConsumer("youtube",
            bootstrap_servers="localhost:29092",
            value_deserializer=forgiving_json_deserializer)

# csvfile = open('results.csv', 'a')

#colocar em uma funcao streaming_table
df = pd.DataFrame(columns=["datetime", "name", "message"])

for msg in consumer:
    msg = json.loads(msg.value)

    #colocar na parte bash
    # with open('json_data.json', 'w') as outfile:
    #     json.dump(msg, outfile)

    #Colocar na parte stream esse frame
    df = pd.concat([df,pd.DataFrame({
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

    # print(msg["author"]["name"],msg["datetime"], msg["message"])
    print()
    
# print(consumer.poll())

