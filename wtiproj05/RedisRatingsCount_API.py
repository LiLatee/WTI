import redis


class RedisRatingsCount:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=2)

    def add_count(self, user_id, genre_count_dataframe):
        self.redis.set(int(user_id), str(genre_count_dataframe))

    def get_count_as_dict(self, user_id):
        return self.redis.get(int(user_id))

    def delete_all_profiles(self):
        pass

    def get_all_profiles(self):
        pass


if __name__ == '__main__':
    pass
