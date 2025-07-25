import pymongo
import psycopg2
import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from model.Tweet import create_tweet_object
from model.User import create_user_object
from model.Hashtag import create_hashtag_object
from cache.custom_cache import Cache

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Connect to Tweet Database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["twitter-database"]
tweets_collection = db["tweets"]


# Connect to User Database
conn = psycopg2.connect(
    dbname="twitter-database",
    user='postgres',
    password='king',
    host='localhost'
)
user_db_cursor = conn.cursor()


@app.get("/hashtags")
async def get_hashtags():
    start_time = time.time()
    if cache.get('trendinghashtags'):
        print(f"Trending hashtags from cache: {time.time() - start_time} seconds")
        return cache.get('trendinghashtags')[0]
    pipeline = [
        {"$unwind": "$hashtag"},
        {"$group": {"_id": "$hashtag", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top_hashtags = list(tweets_collection.aggregate(pipeline))
    top_hashtags_dict = {}
    for hashtag in top_hashtags:
        top_hashtags_dict[hashtag['_id']] = hashtag['count']
    trending_hashtags = top_hashtags_dict
    items = []
    for key,value in trending_hashtags.items():
        items.append(create_hashtag_object(key, value))
    cache.put('trendinghashtags', items)
    print(f"Fetching trending hashtags from database: {time.time() - start_time} seconds")
    return items

@app.get('/recenttweets')
async def get_recent_tweets():
    most_recent_tweets = tweets_collection.find({}).sort("created_at", -1).limit(10)
    tweets = []

    for tweet in most_recent_tweets:
        tweets.append(create_tweet_object(tweet))

    return tweets


@app.get('/trendingtweets')
async def get_trending_tweets():
    start_time = time.time()
    if cache.get('trendingtweets'):
        print(f"Trending tweets from cache: {time.time() - start_time} seconds")
        return cache.get('trendingtweets')[0]
    most_recent_tweets = tweets_collection.find({}).sort("tweet_score", -1).limit(10)
    tweets = []

    for tweet in most_recent_tweets:
        tweets.append(create_tweet_object(tweet))

    cache.put('trendingtweets', tweets)
    print(f"Fetching trending tweets from database: {time.time() - start_time} seconds")
    return tweets

@app.get('/trendingusers')
async def get_trending_users():
    start_time = time.time()
    if cache.get('trendingusers'):
        print(f"Trending users from cache: {time.time() - start_time} seconds")
        return cache.get('trendingusers')[0]
    query = """select id,name,screen_name,verified,location,description,followers_count,friends_count,tweets_count from users 
     order by followers_count DESC,tweets_count DESC 
     limit 10"""

    user_db_cursor.execute(query)
    top_10_users = user_db_cursor.fetchall()

    users = []
    for user in top_10_users:
        users.append(create_user_object(user))
    cache.put('trendingusers', users)
    print(f"Fetching trending users from database: {time.time() - start_time} seconds")
    return users


@app.get('/gettweetsbyuserid')
async def get_recent_tweets(user_id:str=''):
    tweets = []
    user_tweets = tweets_collection.find({"user_screen_name": user_id[1:]}).sort("tweet_score", -1)
    for tweet in user_tweets:
        tweets.append(create_tweet_object(tweet))
    return tweets


@app.get('/filterby')
async def get_filtered_tweets(search:str='', ishashtag: bool = False):
    start_time = time.time()
    if search.startswith("@"):
        if cache.get(search):
            print(f"Cached result in {time.time() - start_time} seconds")
            return cache.get(search)[0]
        search_string = search[1:]
        query = """
                SELECT * FROM users 
                WHERE screen_name LIKE %s 
                ORDER BY followers_count DESC, tweets_count DESC, verified DESC
                LIMIT 10
                """
        user_db_cursor.execute(query, ('%' + search_string + '%',))
        results = user_db_cursor.fetchall()
        users = []
        for user in results:
            users.append(create_user_object(user))
        cache.put(search, users)
        print(f"Fetching from database {time.time() - start_time} seconds")
        return users
    elif ishashtag:
        if cache.get(search):
            print(f"Cached result in {time.time() - start_time} seconds")
            return cache.get(search)[0]
        matched_tweets = tweets_collection.find({"hashtag": {"$in": [search]}}).sort("tweet_score", -1).limit(10)
        tweets = []
        for tweet in matched_tweets:
            tweets.append(create_tweet_object(tweet))
        cache.put(search, tweets)
        print(f"Fetching from database {time.time() - start_time} seconds")
        return tweets

    else:
        if cache.get(search):
            print(f"Cached result in {time.time() - start_time} seconds")
            return cache.get(search)[0]
        matching_tweets = tweets_collection.find({"$text": {"$search": search}}).sort("tweet_score", -1)
        tweets = []
        for tweet in matching_tweets:
            tweets.append(create_tweet_object(tweet))
        cache.put(search, tweets)
        print(f"Fetching from database {time.time() - start_time} seconds")
        return tweets


cache = Cache()
print(cache.last_checkpoint)
