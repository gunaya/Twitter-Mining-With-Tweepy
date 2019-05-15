from pymongo import MongoClient
import json
import re

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'kurang'


client = MongoClient(MONGO_HOST)
db = client[MONGO_DB]
collection = db.chikungunya

data = collection
data_details = data.find()

for i in data_details:
    print(i['text'])