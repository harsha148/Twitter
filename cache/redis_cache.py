import time
import json
import pickle
import os
import redis
from typing import Optional, List, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class RedisCache:
    """
    Redis Cloud cache implementation that maintains the same interface as custom_cache.py
    but uses Redis for distributed caching instead of local memory.
    """
    
    def __init__(self, max_size=10000, evict_strategy='least_accessed', checkpoint_interval=300, ttl=None):
        self.max_size = max_size
        self.evict_strategy = evict_strategy
        self.checkpoint_interval = checkpoint_interval
        self.ttl = ttl
        self.last_checkpoint = time.time()
        
        # Initialize Redis connection
        self.redis_client = self._connect_to_redis()
        
        # Access tracking for LRU eviction (stored in Redis)
        self.access_count_key = "cache:access_count"
        self.cache_size_key = "cache:size"
        
        if self.redis_client is None:
            print("Warning: Redis connection failed. Cache operations will be no-ops.")
    
    def _connect_to_redis(self) -> Optional[redis.Redis]:
        """Connect to Redis Cloud"""
        try:
            # Create Redis client
            redis_base_url = "redis-13781.c273.us-east-1-2.ec2.redns.redis-cloud.com"
            redis_client = redis.Redis(
                host=redis_base_url,
                port=13781,
                decode_responses=True,
                username="default",
                password="6vAXtvrFigwPrxzpYgn0Gbb9mmh3U3Us",
            )
            
            if not redis_url:
                print("REDIS_URL environment variable not set. Using local Redis fallback.")
                redis_url = "redis://localhost:6379"
            
            # redis_client = redis.from_url(redis_url, decode_responses=False)
            
            # Test connection
            redis_client.ping()
            print("Connected to Redis Cloud successfully")
            return redis_client
            
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            return None
    
    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value to bytes for Redis storage"""
        try:
            return pickle.dumps(value)
        except Exception as e:
            print(f"Error serializing value: {e}")
            return pickle.dumps(None)
    
    def _deserialize_value(self, value: bytes) -> Any:
        """Deserialize value from bytes"""
        try:
            return pickle.loads(value)
        except Exception as e:
            print(f"Error deserializing value: {e}")
            return None
    
    def _get_access_count(self, key: str) -> int:
        """Get access count for a key"""
        if self.redis_client is None:
            return 0
        
        try:
            count = self.redis_client.hget(self.access_count_key, key)
            return int(count) if count else 0
        except:
            return 0
    
    def _increment_access_count(self, key: str):
        """Increment access count for a key"""
        if self.redis_client is None:
            return
        
        try:
            self.redis_client.hincrby(self.access_count_key, key, 1)
        except Exception as e:
            print(f"Error incrementing access count: {e}")
    
    def _get_cache_size(self) -> int:
        """Get current cache size"""
        if self.redis_client is None:
            return 0
        
        try:
            size = self.redis_client.get(self.cache_size_key)
            return int(size) if size else 0
        except:
            return 0
    
    def _increment_cache_size(self):
        """Increment cache size counter"""
        if self.redis_client is None:
            return
        
        try:
            self.redis_client.incr(self.cache_size_key)
        except Exception as e:
            print(f"Error incrementing cache size: {e}")
    
    def _decrement_cache_size(self):
        """Decrement cache size counter"""
        if self.redis_client is None:
            return
        
        try:
            self.redis_client.decr(self.cache_size_key)
        except Exception as e:
            print(f"Error decrementing cache size: {e}")
    
    def _evict_if_needed(self, current_key: str):
        """Evict items if cache is full"""
        if self.redis_client is None:
            return
        
        try:
            current_size = self._get_cache_size()
            
            if current_size > self.max_size:
                if self.evict_strategy == 'least_accessed':
                    # Get all keys and their access counts
                    access_counts = self.redis_client.hgetall(self.access_count_key)
                    
                    if access_counts:
                        # Find least accessed key
                        least_accessed_key = min(access_counts.items(), key=lambda x: int(x[1]))[0]
                        
                        if least_accessed_key != current_key:
                            # Remove least accessed key
                            self.redis_client.delete(f"cache:data:{least_accessed_key}")
                            self.redis_client.hdel(self.access_count_key, least_accessed_key)
                            self._decrement_cache_size()
                
                elif self.evict_strategy == 'oldest':
                    # For oldest strategy, we'd need to track timestamps
                    # This is a simplified version - removes a random key
                    keys = self.redis_client.keys("cache:data:*")
                    if keys and len(keys) > 1:
                        key_to_remove = keys[0].decode('utf-8').replace("cache:data:", "")
                        if key_to_remove != current_key:
                            self.redis_client.delete(f"cache:data:{key_to_remove}")
                            self.redis_client.hdel(self.access_count_key, key_to_remove)
                            self._decrement_cache_size()
        
        except Exception as e:
            print(f"Error during eviction: {e}")
    
    def get(self, key: str) -> Optional[List[Any]]:
        """
        Get value from cache. Returns a list of values (maintaining compatibility with original cache).
        """
        if self.redis_client is None:
            return None
        
        try:
            # Normalize key
            if not key.startswith('#'):
                key = key.lower()
            
            # Check for exact match first
            if key[0].isdigit() or key.startswith('#'):
                # Exact match
                value = self.redis_client.get(f"cache:data:{key}")
                if value:
                    self._increment_access_count(key)
                    return [self._deserialize_value(value)]
                return None
            else:
                # Pattern match (similar to original cache behavior)
                pattern = f"cache:data:*{key}*"
                matching_keys = self.redis_client.keys(pattern)
                
                if not matching_keys:
                    return None
                
                results = []
                for redis_key in matching_keys:
                    key_name = redis_key.decode('utf-8').replace("cache:data:", "")
                    value = self.redis_client.get(redis_key)
                    if value:
                        self._increment_access_count(key_name)
                        results.append(self._deserialize_value(value))
                
                return results if results else None
        
        except Exception as e:
            print(f"Error getting from cache: {e}")
            return None
    
    def put(self, key: str, value: Any):
        """
        Put value into cache
        """
        if self.redis_client is None:
            return
        
        try:
            # Normalize key
            if not key.startswith('#'):
                key = key.lower()
            
            # Serialize value
            serialized_value = self._serialize_value(value)
            
            # Store in Redis with TTL if specified
            redis_key = f"cache:data:{key}"
            
            if self.ttl:
                self.redis_client.setex(redis_key, self.ttl, serialized_value)
            else:
                self.redis_client.set(redis_key, serialized_value)
            
            # Initialize access count
            self.redis_client.hset(self.access_count_key, key, 0)
            
            # Increment cache size
            self._increment_cache_size()
            
            # Evict if needed
            self._evict_if_needed(key)
            
            # Checkpoint logic (simplified for Redis)
            current_time = time.time()
            if (current_time - self.last_checkpoint) > self.checkpoint_interval:
                self.last_checkpoint = current_time
                print(f"Cache checkpoint: {self._get_cache_size()} items")
        
        except Exception as e:
            print(f"Error putting to cache: {e}")
    
    def expire_items(self):
        """
        Redis handles TTL automatically, so this is mostly for compatibility
        """
        if self.redis_client is None:
            return
        
        try:
            # Redis automatically expires items based on TTL
            # This method is kept for compatibility with original cache interface
            pass
        except Exception as e:
            print(f"Error expiring items: {e}")
    
    def print_cache(self):
        """Print cache statistics"""
        if self.redis_client is None:
            print("Cache: Redis not connected")
            return
        
        try:
            # Get all cache keys
            keys = self.redis_client.keys("cache:data:*")
            key_names = [key.decode('utf-8').replace("cache:data:", "") for key in keys]
            
            print('Cache:')
            for key in key_names:
                print(f"{key}")
            
            used_space = len(key_names)
            remaining_space = self.max_size - used_space
            print(f"Cache size: {used_space}")
            print(f"Remaining space: {remaining_space}")
            
            # Print Redis info
            info = self.redis_client.info()
            print(f"Redis memory used: {info.get('used_memory_human', 'N/A')}")
        
        except Exception as e:
            print(f"Error printing cache: {e}")
    
    def clear_cache(self):
        """Clear all cache data"""
        if self.redis_client is None:
            return
        
        try:
            # Delete all cache keys
            keys = self.redis_client.keys("cache:data:*")
            if keys:
                self.redis_client.delete(*keys)
            
            # Clear access counts
            self.redis_client.delete(self.access_count_key)
            
            # Reset cache size
            self.redis_client.set(self.cache_size_key, 0)
            
            print("Cache cleared successfully")
        
        except Exception as e:
            print(f"Error clearing cache: {e}")
    
    def get_cache_stats(self) -> dict:
        """Get cache statistics"""
        if self.redis_client is None:
            return {"error": "Redis not connected"}
        
        try:
            info = self.redis_client.info()
            return {
                "cache_size": self._get_cache_size(),
                "max_size": self.max_size,
                "redis_memory": info.get('used_memory_human', 'N/A'),
                "redis_connected_clients": info.get('connected_clients', 0),
                "redis_uptime": info.get('uptime_in_seconds', 0)
            }
        except Exception as e:
            return {"error": str(e)}

# For backward compatibility, you can use this alias
Cache = RedisCache 