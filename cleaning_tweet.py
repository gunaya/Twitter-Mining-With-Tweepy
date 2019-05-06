from pymongo import MongoClient
import json
import re

MONGO_HOST = 'mongodb://localhost/'
MONGO_DB = 'testing'
FLAG = 0;

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
                locations[x] = location_filter(locations[x])
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

    print('None : {}'.format(none_count))

def location_filter(kota):
    with open('countries.json') as f:
        data = json.load(f)

    # Inisialisasi Singkatan
    US = ['usa', 'us', 'america', 'fl', 'nyc', 'dc', 'oh', 'tx', 'ma', 'mn']
    country = ''

    # filepath = 'City Lists/CITIES2.TXT'
    # with open(filepath) as fp:
    #     for cnt, line in enumerate(fp):
    #         line = line.strip().split(None, 1)
    #         if not len(line) == 1:
    #             x = line[1]
    #             if any(re.findall('|'.join(US), kota.lower())):
    #                 country = 'United States'
    #                 break
    #             elif kota.lower() == 'bc':
    #                 country = 'Canada'
    #                 break
    #             elif kota.lower() == 'uk' or kota.lower() == 'u.k':
    #                 country = 'United Kingdom'
    #                 break
    #             elif kota.lower() == x.lower():
    #                 country = line[0]
    #                 print("found")
    #                 break
    #             else:
    #                 country = kota
    #                 break

    for countries in data:
        if kota.lower() == countries.lower():
            country = countries
        else:
            for x in data[countries]:
                if kota.lower() == x.lower():
                    FLAG = 1;
                    print('found')
                    country = countries
                    break
                elif any(re.findall('|'.join(US), kota.lower())):
                    FLAG = 1;
                    country = 'United States'
                    break
                elif kota.lower() == 'bc':
                    FLAG = 1;
                    country = 'Canada'
                    break
                elif kota.lower() == 'uk' or kota.lower() == 'u.k' or kota.lower() == 'england':
                    FLAG = 1;
                    country = 'United Kingdom'
                    break
                elif kota.lower() != x.lower():
                    FLAG = 0;
                    country = kota
                    break
                # elif kota.lower() != '' and kota.lower() != x.lower():
                #     country = kota.lower()
                #     print("tessse")
                #     break
    return country

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