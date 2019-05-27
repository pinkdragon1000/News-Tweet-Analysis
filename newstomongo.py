import twitter
import tweepy
import json
import pymongo 

def oauth_login():
    # Twitter App Credentials
    CONSUMER_KEY = 'rU2DgtkQVDmxsxiM0UqOq39Tk'
    CONSUMER_SECRET = 'jPBVLlJVtLKqZpKDRX34Dlj7UEqEzNsLH3Qxd1SihWtZtnKBms'
    OAUTH_TOKEN = '1722919758-1r0sZmPrTKO83MwkvE7hGmMKsiXkD76EiXl3Ctb'
    OAUTH_TOKEN_SECRET = 'FG0P91BaA6nM2a0oZWemDvbpSj0lhL6EOGl40De1RSqma'
    
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    
    api = tweepy.API(auth)
    return api

# Create API object
api = oauth_login()  
print(api)


def save_to_mongo(data, mongo_db, mongo_db_coll, **mongo_conn_kw):
    
    # Connects to the MongoDB server running on 
    # localhost:27017 by default
    
    client = pymongo.MongoClient(**mongo_conn_kw)
    
    # Get a reference to a particular database
    
    db = client[mongo_db]
    
    # Reference a particular collection in the database
    
    coll = db[mongo_db_coll]
    
    # Perform a bulk insert and  return the IDs
    try:
        return coll.insert_many(data)
    except:
        return coll.insert_one(data)

def load_from_mongo(mongo_db, mongo_db_coll, return_cursor=False,
                    criteria=None, projection=None, **mongo_conn_kw):
    
    # Optionally, use criteria and projection to limit the data that is 
    # returned as documented in 
    # http://docs.mongodb.org/manual/reference/method/db.collection.find/
    
    # Consider leveraging MongoDB's aggregations framework for more 
    # sophisticated queries.
    
    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    coll = db[mongo_db_coll]
    
    if criteria is None:
        criteria = {}
    
    if projection is None:
        cursor = coll.find(criteria)
    else:
        cursor = coll.find(criteria, projection)

    # Returning a cursor is recommended for large amounts of data
    
    if return_cursor:
        return cursor
    else:
        return [ item for item in cursor ]


#override tweepy.StreamListener to add logic to on_status
counter = 1
host = 'mongodb+srv://USERNAME:PASSWORD@cluster0-lhmfm.mongodb.net/test?retryWrites=true'#replace this with your connection string

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        global counter
        #print('status', status)
        if status.lang == "en":
            if (not status.retweeted) and ('RT @' not in status.text):
                print(counter, status.text)
                save_to_mongo(status._json, 'search_results', 'news_stream', host=host)
                counter = counter +1

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)


myStream.filter(track=['#news'])
