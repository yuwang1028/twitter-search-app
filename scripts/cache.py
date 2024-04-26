import json
import os
from collections import OrderedDict
from datetime import datetime

class CacheManager:
    def __init__(self, cache_file="diskCache.json", max_size=1024):
        """
        Initializes a new instance of the DiskLRUCache class.

        Parameters:
        cache_file (str): The path to the file that will be used for caching.
        max_size (int): The maximum number of entries that can be stored in the cache.
        """
        self.cache_file = cache_file
        self.max_size = max_size
        self.cache = self.loadCache()

    def loadCache(self):
        """
        Loads the cache from the file on disk, or initializes a new one if the file doesn't exist.
        """
        try:
            if not os.path.exists('./data'):
                os.makedirs('./data')
            
            if os.path.exists(os.path.join('./data', self.cache_file)):
                with open(os.path.join('./data', self.cache_file), 'r') as f:
                    return OrderedDict(json.load(f))
            else:
                return OrderedDict()
        except Exception as e:
            print(f'Cache Load Failed: {e}. Initializing empty cache.')
            return OrderedDict()

    async def saveCache(self):
        """
        Saves the cache to the file on disk.
        """
        try:
            with open(os.path.join('./data', self.cache_file), 'w') as f:
                json.dump(self.cache, f)
            print('Cache saved successfully.')
        except Exception as e:
            print(f'Cache Save Failed: {e}.')

    def getQuery(self, key):
        """
        Retrieves a value from the cache by key.
        """
        key = json.dumps(key) if isinstance(key, dict) else key
        return self.cache.get(key)

    def putQuery(self, key, value):
        """
        Adds or updates a value in the cache by key.
        """
        key = json.dumps(key) if isinstance(key, dict) else key
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[key] = value

    def delQuery(self, key):
        """
        Deletes a value from the cache by key.
        """
        key = json.dumps(key) if isinstance(key, dict) else key
        self.cache.pop(key, None)

    def clear(self):
        """
        Clears the entire cache.
        """
        self.cache.clear()
        print('Cache cleared.')

if __name__ == "__main__":
    cache_manager = CacheManager()
    # Test the cache manager
    test_key = 'test'
    test_value = 'value'
    cache_manager.putQuery(test_key, test_value)
    print(f"Retrieved from cache: {cache_manager.getQuery(test_key)}")
    cache_manager.delQuery(test_key)
    print(f"After deletion: {cache_manager.getQuery(test_key)}")
