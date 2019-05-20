import redis
import json

class RedisCountAll:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=2)

    def set_count_of_all_users(self, genre_count_dict):
        self.redis.set('all', json.dumps(genre_count_dict))

    def get_count_of_all_users_as_dict(self):
        return json.loads(self.redis.get('all'))

    def delete_all_profiles(self):
        pass

    def get_all_profiles(self):
        pass

    def delete_all(self):
        self.redis.flushall()

if __name__ == '__main__':
    r = RedisCountAll()

    r.set_count_of_all_users(genre_count_dict="{'genre_Action': 55, 'genre_Adventure': 11")
    result = r.get_count_of_all_users_as_dict()
    print(result)

