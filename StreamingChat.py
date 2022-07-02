import pytchat
import time

import os
import json
import pandas as pd 

# chat = pytchat.create(video_id="uIx8l2xlYVY")
# while chat.is_alive():
#     for c in chat.get().items:
#         print(c.json())
#     time.sleep(5)
#     '''
#     # Each chat item can also be output in JSON format.
#     for c in chat.get().items:
#         print(c.json())
#     '''  

# print(os.getcwd("datalake"))


# json.load(".\\datalake\\bronze\\lofi-hiphop\\data_concentrated.txt")

with open('.\\datalake\\bronze\\lofi-hiphop\\data_concentrated.txt') as f:
   data = json.load(f)

print(data)

# df = pd.read_json(".\\datalake\\bronze\\lofi-hiphop\\data_concentrated.txt")