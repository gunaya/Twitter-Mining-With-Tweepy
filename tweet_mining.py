import pandas
import json
import jsonpickle
import time

import datetime

from pymongo import MongoClient
import tweepy

#consumer
CONSUMER_KEY = 'HxVfkRtGy4EFwWAVAz1VbnDvp'
CONSUMER_SECRET = 'r5Bg9JESYAoeLn4dkrLTAFY6CxxcM2MUdGUVRswkv8vQZ3n1bw'

#token
ACCESS_TOKEN = '1331219286-piOcHazoDlHmgV3aial3AM87dAYtUat1hvHRnY9'
ACCESS_SECRET = 'EaYxOc7loNyaxyOQHna7mX1qiSCPXF44b6eUrRiWgY5YI'

#host mongo
MONGO_HOST = 'mongodb://localhost/'

#API setup
def twitter_auth():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit_notify=False, wait_on_rate_limit=False, retry_count=5, retry_delay=5)
    print('auth success')
    return api

def limit_handled(cursor):
    backoff_counter = 1
    while True:
        try:
            yield cursor.next()
        except tweepy.TweepError as e:
            print(e.reason)
            time.sleep(60 * backoff_counter)
            backoff_counter += 1
            continue

#get tweet
def get_tweet(db, collection_name, filepath, api, query, max_tweets):
    collection = db[collection_name]
    tweetCount = 0

    with open("./json/"+filepath, 'a+') as f:
        for tweet in limit_handled(tweepy.Cursor(api.search, q=query, since="2019-4-30", until="2019-5-1", tweet_mode='extended').items(max_tweets)):
            if (not tweet.retweeted) and ('RT @' not in tweet.full_text):
                f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
                #json.dump(tweet._json, f, indent=2)
                data = {}
                data['created_at'] = tweet.created_at
                data['geo'] = tweet.geo
                data['id'] = tweet.id
                data['username'] = tweet.user.name
                data['text'] = tweet.full_text
                data['location'] = tweet.user.location
                data['coordinates'] = tweet.coordinates

                collection.insert(data)

                tweetCount += 1
                print("Query = {}. Downloaded {} tweets".format(query, tweetCount))

#MAIN
def main():
    #call twitter_auth()
    api = twitter_auth()
    #connect mongodb
    client = MongoClient(MONGO_HOST)
    db = client.db_twitter
    #read query.txt
    file_query = open("query.txt", "r")
    for i in file_query:
        JSON_DATA = {}
        date_now = datetime.datetime.now()
        query = i.split(",")
        collection_name = query[0]
        collection_name = collection_name.replace('"', '')
        path = (collection_name.rstrip()) + '_{}_{}-{}-{}-{}.json'.format(datetime.date.today(),
                                                                   date_now.hour,
                                                                   date_now.minute,
                                                                   date_now.second,
                                                                   date_now.microsecond)
        for q in query:
            print(q)
            get_tweet(db, collection_name, path, api, q, 1000000)


if __name__ == "__main__":
    main()

