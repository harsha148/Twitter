from datetime import datetime

class Tweet:
    def __init__(self,user_name,user_id,created_at,text,retweet_count,likes_count,hashtags):
        self.user_name = user_name
        self.user_id = user_id
        self.created_at = created_at
        self.text = text
        self.retweet_count = retweet_count
        self.likes_count = likes_count
        self.hashtags = hashtags


def create_tweet_object(tweet):
    return Tweet(user_name=tweet["user_name"], user_id="@" + tweet["user_screen_name"],
          created_at=datetime.strptime(tweet["created_at"], "%Y-%m-%d %H:%M:%S").strftime(
              "%d %b, %Y %I:%M%p"),
          text=tweet["text"], retweet_count=tweet["retweet_count"], likes_count=tweet["likes_count"],
          hashtags=tweet["hashtag"])
