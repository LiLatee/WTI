import redis


class RedisProfiles:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=1)


    def add_profile(self, userID, rating):
        print(rating)
        self.redis.set(int(userID), str(rating))

    def delete_all(self):
        self.redis.delete(self.list_name)

    def get_all(self):
        return self.redis.lrange(self.list_name, 0, -1)


if __name__ == '__main__':
    r = RedisProfiles()
    r.add_profile('')
    r.delete_all()
    result = r.get_all()
    print(result)