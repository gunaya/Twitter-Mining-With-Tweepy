from pymongo import MongoClient
import json

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'testing'

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

    tweet_count = 1
    for i in data_details:

        locations = i['location']
        locations = locations.split(',')

        for x in range(len(locations)):
            locations[x] = locations[x].replace(' ', '')
            locations[x] = location_filter(locations[x])
            if locations[x] == '':
                locations[x] = 'none'

        new_location = 'none'
        for x in range(len(locations)):
            if locations[x] != 'none':
                new_location = locations[x]

        tweet_count += 1

        data = {}
        data['tweet_id'] = i['id']
        data['tweet_location'] = new_location
        data['created_at'] = i['created_at']

        new_collection.insert(data)

        print("{} tweet successfully imported into {} collection on MongoDB".format(tweet_count, col_name))

def location_filter(kota):
    with open('countries.json') as f:
        data = json.load(f)
    # kota = 'Visakhapatnam'
    country = ''
    for countries in data:
        if kota.lower() == countries.lower():
            country = countries
        else:
            for x in data[countries]:
                if kota.lower() == 'usa' or kota.lower() == 'u.s.a' or kota.lower() == 'us' or kota.lower() == 'nyc':
                    country = 'United States'
                    break
                elif kota.lower() == 'uk' or kota.lower() == 'u.k':
                    country = 'United Kingdom'
                    break
                elif kota.lower() == x.lower():
                    country = countries
                    break
    return country

if __name__ == "__main__":
    query = ['chikungunya', 'dengue', 'ebola', 'hiv', 'malaria', 'measles', 'mers', 'polio', 'rabies', 'tuberculosis', 'yellowfever', 'zika']

    for q in query:
        main(q)
    # location_filter()
