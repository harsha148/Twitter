import time
import pickle
import os


class Cache:
    def __init__(self, max_size=10000, evict_strategy='least_accessed', checkpoint_interval=300, ttl=None):
        self.max_size = max_size
        self.evict_strategy = evict_strategy
        self.checkpoint_interval = checkpoint_interval
        self.ttl = ttl
        self.cache = {}
        self.access_count = {}
        self.last_checkpoint = time.time()

        if os.path.exists('cache/cache.checkpoint'):
            self.load_from_checkpoint('cache/cache.checkpoint')

    def load_from_checkpoint(self, checkpoint_file):
        with open(checkpoint_file, 'rb') as f:
            self.cache, self.access_count = pickle.load(f)

    def save_to_checkpoint(self, checkpoint_file):
        with open(checkpoint_file, 'wb') as f:
            pickle.dump((self.cache, self.access_count), f)

    def get(self, key):
        # Clean up expired items
        self.expire_items()
        if key[0].isdigit() or key.startswith('#'):
            if key not in self.cache:
                return None
            similar_keys = [key]

        else:
            similar_keys = []
            for k in self.cache:
                if key in k:
                    similar_keys.append(k)

            if len(similar_keys) == 0:
                return None

        if self.ttl is not None and (time.time() - self.cache[key]['timestamp']) > self.ttl:
            del self.cache[key]
            del self.access_count[key]
            return None

        for i in similar_keys:
            self.access_count[i] += 1

            if self.evict_strategy == 'least_accessed':
                least_accessed_key = min(self.access_count, key=self.access_count.get)
                if len(self.cache) > self.max_size and key != least_accessed_key:
                    del self.cache[least_accessed_key]
                    del self.access_count[least_accessed_key]

        return [self.cache[k]['value'] for k in similar_keys]

    def put(self, key, value):
        if not key.startswith('#'):
            key = key.lower()
        self.cache[key] = {'value': value, 'timestamp': time.time()}
        self.access_count[key] = 0
        if len(self.cache) > self.max_size:
            if self.evict_strategy == 'least_accessed':
                least_accessed_key = min(self.access_count, key=self.access_count.get)
                del self.cache[least_accessed_key]
                del self.access_count[least_accessed_key]
            elif self.evict_strategy == 'oldest':
                oldest_key = min(self.cache, key=lambda k: self.cache[k]['timestamp'])
                del self.cache[oldest_key]
                del self.access_count[oldest_key]

        if (time.time() - self.last_checkpoint) > self.checkpoint_interval:
            cache_file_path = os.path.join('cache/', 'cache.checkpoint')
            self.save_to_checkpoint(cache_file_path)
            self.last_checkpoint = time.time()

    def expire_items(self):
        if self.ttl:
            current_time = time.time()
            expired_keys = [key for key, val in self.cache.items() if (current_time - val['timestamp']) > self.ttl]
            for key in expired_keys:
                del self.cache[key]
                del self.access_count[key]

    def print_cache(self):
        print('Cache:')
        for key, value in self.cache.items():
            print(f"{key}")
        used_space = len(self.cache)
        remaining_space = self.max_size - used_space
        print(f"Cache size: {used_space}")
        print(f"Remaining space: {remaining_space}")
