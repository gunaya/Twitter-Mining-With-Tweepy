from pymongo import MongoClient
from bson.codec_options import CodecOptions
import pytz

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'db_twitter'

timezone = pytz.timezone('Asia/Makassar')

client = MongoClient(MONGO_HOST)
db = client['Hitung']
collection = db['2'].with_options(codec_options=CodecOptions(tz_aware=True, tzinfo=timezone))
data = collection
data_details = data.find()

for i in data_details:
    print(i['created_at'])
