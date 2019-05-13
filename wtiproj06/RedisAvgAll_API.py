import redis
import json

class RedisAvgAll:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=0)

        self.list_of_all_genres = ['genre_action', 'genre_adventure',
                                   'genre_animation', 'genre_children', 'genre_comedy', 'genre_crime',
                                   'genre_documentary',
                                   'genre_drama', 'genre_fantasy',
                                   'genre_film_noir', 'genre_horror', 'genre_musical', 'genre_mystery', 'genre_romance',
                                   'genre_sci_fi', 'genre_thriller',
                                   'genre_war', 'genre_western']

    def set_avg_of_all_users(self, avg_all_dict):
        self.redis.set("avg-all", json.dumps(avg_all_dict))

    def get_avg_of_all_users_as_dict(self):
        result = self.redis.get('avg-all')
        if result == None:
            result = dict.fromkeys(self.list_of_all_genres, 2.5)

        result = json.loads(result)
        return result


if __name__ == '__main__':
    r = RedisAvgAll()

    avg_all_dict = '''{"genre_Adventure": 2.33, "genre_comedy": 5.7856, "genre_drama": 0,
     "genre_fantasy": 0, "genre_mystery": 0, "genre_Romance": 1, "genre_sci_fi": 0, "genre_thriller": 0,
     "genre_war": 0}'''
    r.set_avg_of_all_users(avg_all_dict=avg_all_dict)

    r.get_avg_of_all_users_as_dict()
    result = r.get_avg_of_all_users_as_dict()
    print(result)

