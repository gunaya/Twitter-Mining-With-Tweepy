from pymongo import MongoClient
from pyspark import SparkContext, SparkConf

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'disease'
loc = []

def get_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client[MONGO_DB]
    collection = db[collection_name]
    return collection


def main(collection_name):
    # get json data from MongoDB
    data = get_json(collection_name)
    data_details = data.find()
    # col_name = collection_name.replace("#",'')

    for i in data_details:
        if i['location'] != '':
            loc.append(i['location'])
    # if loc != '':
    #     location.append(loc)
    print(loc)

    # country = loc.groupby('STNAME')['COUNTY'].nunique()
    # print(country)
    conf = SparkConf().setAppName("count").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # inputWords = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]

    wordRdd = sc.parallelize(loc)
    print("Count: {}".format(wordRdd.count()))

    worldCountByValue = wordRdd.countByValue()
    print("CountByValue: ")
    for word, count in worldCountByValue.items():
        print("{} : {}".format(word, count))


if __name__ == "__main__":
    main("chikungunya")

