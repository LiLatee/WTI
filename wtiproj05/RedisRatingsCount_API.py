import redis
import json

class RedisRatingsCount:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=2)

    def set_count_for_user(self, user_id, genre_count_dict):
        self.redis.set(int(user_id), json.dumps(genre_count_dict))

    def set_count_of_all_users(self, genre_count_dict):
        self.redis.set('all', json.dumps(genre_count_dict))

    def get_count_of_user_as_dict(self, user_id):
        try:
            result = json.loads(self.redis.get(int(user_id)))
        except TypeError:
            result = {}
        return result

    def get_count_of_all_users_as_dict(self):
        return json.loads(self.redis.get('all'))

    def delete_all_profiles(self):
        pass

    def get_all_profiles(self):
        pass


if __name__ == '__main__':
    r = RedisRatingsCount()

    r.set_count_for_user(user_id=5, genre_count_dict="{'genre_Action': 5, 'genre_Adventure': 11")
    result = r.get_count_of_user_as_dict(5)
    print(result)

    r.set_count_of_all_users(genre_count_dict="{'genre_Action': 55, 'genre_Adventure': 11")
    result = r.get_count_of_all_users_as_dict()
    print(result)

