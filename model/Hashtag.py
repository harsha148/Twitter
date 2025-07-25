class Hashtag:
    def __init__(self, hashtag, count):
        self.hashtag = hashtag
        self.count = count


def create_hashtag_object(key, value):
    key = "#" + key
    value_k = str(round(value / 1000, 2)) + "K"
    return Hashtag(key, value_k)
