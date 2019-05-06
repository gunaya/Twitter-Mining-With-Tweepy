from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
import json

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'data-penyakit'
loc = []
data_eror = []

def get_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client[MONGO_DB]
    collection = db[collection_name]
    return collection
# def save_data(data):
#     with open('data_location.json', 'w') as outfile:
#         json.dump(data, outfile)
#         print("sukses")
#     return data

def save_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client["db_test"]
    collection = db[collection_name]
    return collection

def main(collection_name):
    # get json data from MongoDB
    data = get_json(collection_name)
    data_details = data.find()
    # col_name = collection_name.replace("#",'')

    for i in data_details:



        if i["tweet_location"] == 'none':
            data_eror.append(i['tweet_location'])
        else:
            loc.append(i['tweet_location'])
    # if loc != '':
    #     location.append(loc)
    # print(data_eror)
    # print("=================================================================")
    # print(loc)

    # country = loc.groupby('STNAME')['COUNTY'].nunique()
    # print(country)
    conf = SparkConf().setAppName("count").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # inputWords = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]

    wordRdd = sc.parallelize(loc)
    # print("Count: {}".format(wordRdd.count()))

    worldCountByValue = wordRdd.countByValue()
    print("CountByValue: ")
    data_count = {}
    # total = []
    for word, count in worldCountByValue.items():
        print("{} : {}".format(word, count))
        data_count['nama_location']=format(word)
        data_count['total']=format(count)
        print("------------------------------")
        print(data_count)
        simpan_json = save_json("location")
        data = {}
        data['nama_location'] = format(word)
        data['total']=int(format(count))

        simpan_json.insert(data)
    # print("----------")
    # print(data_count)

    # print(total)

if __name__ == "__main__":
    main("hiv")

