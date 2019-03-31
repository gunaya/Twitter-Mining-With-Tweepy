from pymongo import MongoClient
MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'disease'

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


    for i in data_details:
        print(i)

if __name__ == "__main__":
    main("hiv")