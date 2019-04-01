from pymongo import MongoClient
MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'db_twitter'

def get_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client[MONGO_DB]
    collection = db[collection_name]
    return collection

def save_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client["clean_tweet"]
    collection = db[collection_name]
    return collection

def main(collection_name):
    # get json data from MongoDB
    data = get_json(collection_name)
    data_details = data.find()
    col_name = collection_name.replace("#",'')
    new_collection = save_json(col_name)

    tweet_count = 0
    for i in data_details:
        data = {}
        data['tweet_id'] = i['id']
        data['tweet_location'] = i['location']
        data['created_at'] = i['created_at']

        new_collection.insert(data)
        tweet_count += 1
        print("{} tweet successfully imported into {} collection on MongoDB".format(tweet_count, col_name))

if __name__ == "__main__":
    main("#zika")