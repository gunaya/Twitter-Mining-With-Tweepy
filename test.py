from pymongo import MongoClient
import json
import re

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'testing'

def get_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client[MONGO_DB]
    collection = db[collection_name]
    return collection

def save_json(collection_name):
    client = MongoClient(MONGO_HOST)
    db = client["hasil"]
    collection = db[collection_name]
    return collection

def main(collection_name):
    FLAG = 0;
    # get json data from MongoDB

    data = get_json(collection_name)
    data_details = data.find()
    col_name = collection_name.replace("#",'')
    new_collection = save_json(col_name)

    tweet_count = 1
    none_count = 0
    for i in data_details:

        locations = i['location']
        locations = locations.split(',')

        for x in range(len(locations)):
            locations[x] = locations[x].strip()
            locations[x] = locations[x].replace('.','')
            if locations[x].isalpha() == True:
                locations[x], FLAG = location_filter(locations[x])
            else:
                locations[x] = ''

            if locations[x] == '':
                locations[x] = 'none'
                none_count += 1

        new_location = 'none'
        for x in range(len(locations)):
            if locations[x] != 'none':
                if new_location == 'none':
                    new_location = locations[x]
                else:
                    if FLAG == 1:
                        if new_location != locations[x]:
                            new_location = locations[x]
                    elif FLAG == 0:
                        new_location = new_location

        # save JSON data into new Collection on MongoDB
        data = {}
        data['tweet_id'] = i['id']
        data['tweet_location'] = new_location
        data['created_at'] = i['created_at']

        # new_collection.insert(data)

        # print("{} tweet successfully imported into {} collection on MongoDB".format(tweet_count, col_name))
        print(tweet_count)
        print(new_location)
        tweet_count += 1

def location_filter(kota):
    FLAG = 0;
    with open('countries.json') as f:
        data = json.load(f)

    # Inisialisasi Singkatan
    US = ['usa', 'us', 'america', 'fl', 'nyc', 'dc', 'oh', 'tx', 'ma', 'mn', 'ny']
    country = ''

    for countries in data:
        if kota.lower() == countries.lower():
            FLAG = 1;
            country = countries
        else:
            for x in data[countries]:
                if kota.lower() == x.lower():
                    FLAG = 1;
                    country = countries
                    break
    if country == '':
        if any(re.findall('|'.join(US), kota.lower())):
            FLAG = 1;
            country = 'United States'
        elif kota.lower() == 'bc':
            FLAG = 1;
            country = 'Canada'
        elif kota.lower() == 'uk' or kota.lower() == 'u.k' or kota.lower() == 'england':
            FLAG = 1;
            country = 'United Kingdom'
        elif kota.lower() is not None:
            FLAG = 1;
            country = kota
                    # elif kota.lower() != '' and kota.lower() != x.lower():
                    #     country = kota.lower()
                    #     print("tessse")
                    #     break
    return country, FLAG

if __name__ == "__main__":
    # query = ['chikungunya', 'dengue', 'ebola', 'hiv', 'malaria', 'measles', 'mers', 'polio', 'rabies', 'tuberculosis', 'yellowfever', 'zika']

    # for q in query:
    #     main(q)
    # location_filter()

    main('yellowfever')

    # with open('countries.json') as f:
    #     data = json.load(f)
    # kota = 'London'
    # for countries in data:
    #     if kota.lower() == countries.lower():
    #         country = countries
    #     else:
    #         for x in data[countries]:
    #             if kota.lower() == x.lower():
    #                 FLAG = 1;
    #                 print('found')
    #                 country = countries
    #                 print(countries)
    #                 break