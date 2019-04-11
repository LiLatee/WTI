import redis
import numpy as np


class RedisProfiles:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=1)

    def add_profile(self, user_id, rating_dict):
        self.redis.set(int(user_id), str(rating_dict))

    def get_profile_as_dict(self, user_id):
        return self.redis.get(int(user_id))

    def delete_all_profiles(self):
        pass

    def get_all_profiles(self):
        pass


if __name__ == '__main__':
    pass