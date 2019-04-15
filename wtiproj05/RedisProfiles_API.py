import redis
import json


class RedisProfiles:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=1)

    def set_profile(self, user_id, profile_dict):
        self.redis.set(int(user_id), json.dumps(profile_dict))

    def get_profile_as_dict(self, user_id):
        return json.loads(self.redis.get(user_id))

    def delete_all_profiles(self):
        pass

    def get_all_profiles(self):
        pass


if __name__ == '__main__':
    pass