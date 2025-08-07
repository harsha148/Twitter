import pymongo
import time
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from dotenv import load_dotenv

from model.Tweet import create_tweet_object
from model.User import create_user_object
from model.Hashtag import create_hashtag_object
from cache.custom_cache import Cache

# Load environment variables
load_dotenv()

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
client = pymongo.MongoClient("mongodb+srv://sk2953:SxVbw2sAorNNdbHJ@twitter.3lvckfi.mongodb.net/?retryWrites=true&w=majority&appName=Twitter")
db = client["twitter-database"]
tweets_collection = db["tweets"]


# Connect to User Database (Supabase)
def connect_to_supabase():
    """Connect to Supabase database"""
    try:
        supabase_url = 'https://krvusrtfpyfuxdpqtxww.supabase.co'
        supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtydnVzcnRmcHlmdXhkcHF0eHd3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM0MDUyMTYsImV4cCI6MjA2ODk4MTIxNn0.vOY8_ZY8SEZ5TIMihgnIfDL5jdEVvYhQM5ap_r8mrOg'
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set")
        
        supabase: Client = create_client(supabase_url, supabase_key)
        print("Connected to Supabase database")
        return supabase
    except Exception as e:
        print(f"Error occurred while connecting to Supabase: {e}")
        return None

# Initialize Supabase connection
supabase = connect_to_supabase()


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
    
    try:
        # Use Supabase to query users table
        response = supabase.table('users').select(
            'id,name,screen_name,verified,location,description,followers_count,friends_count,tweets_count'
        ).order('followers_count', desc=True).order('tweets_count', desc=True).limit(10).execute()
        
        users = []
        for user_data in response.data:
            # Convert Supabase response to tuple format expected by create_user_object
            user_tuple = (
                user_data.get('id'),
                user_data.get('name'),
                user_data.get('screen_name'),
                user_data.get('verified'),
                user_data.get('location'),
                user_data.get('description'),
                user_data.get('followers_count'),
                user_data.get('friends_count'),
                user_data.get('tweets_count')
            )
            users.append(create_user_object(user_tuple))
        
        cache.put('trendingusers', users)
        print(f"Fetching trending users from database: {time.time() - start_time} seconds")
        return users
    except Exception as e:
        print(f"Error fetching trending users: {e}")
        return []


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
        
        try:
            # Use Supabase to search users by screen_name
            response = supabase.table('users').select('*').ilike('screen_name', f'%{search_string}%').order('followers_count', desc=True).order('tweets_count', desc=True).order('verified', desc=True).limit(10).execute()
            
            users = []
            for user_data in response.data:
                # Convert Supabase response to tuple format expected by create_user_object
                user_tuple = (
                    user_data.get('id'),
                    user_data.get('name'),
                    user_data.get('screen_name'),
                    user_data.get('verified'),
                    user_data.get('location'),
                    user_data.get('description'),
                    user_data.get('followers_count'),
                    user_data.get('friends_count'),
                    user_data.get('tweets_count')
                )
                users.append(create_user_object(user_tuple))
            
            cache.put(search, users)
            print(f"Fetching from database {time.time() - start_time} seconds")
            return users
        except Exception as e:
            print(f"Error searching users: {e}")
            return []
            
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
