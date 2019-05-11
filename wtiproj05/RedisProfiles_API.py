import redis
import json


class RedisProfiles:
    def __init__(self):
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=1)

    def set_profile(self, user_id, profile_dict):
        self.redis.set(int(user_id), json.dumps(profile_dict))

    def get_profile_as_dict(self, user_id):
        try:
            result = json.loads(self.redis.get(user_id))
        except TypeError:
            result = {}
        return result

    def delete_all_profiles(self):
        pass

    def get_all_profiles(self):
        pass


if __name__ == '__main__':
    r = RedisProfiles()
    r.set_profile(user_id=4 ,profile_dict="{'genre_Action': -0.06834349593495936, 'genre_Adventure': 0.16729323308270683, 'genre_Animation': 0.40000000000000036, 'genre_Children': 1.0, 'genre_Comedy': 0.5916666666666663, 'genre_Crime': -0.0038461538461538325, 'genre_Documentary': 0, 'genre_Drama': 0.31874145006839916, 'genre_Fantasy': 0.17499999999999982, 'genre_Film-Noir': -0.25, 'genre_Horror': 0.0, 'genre_Musical': -0.5, 'genre_Mystery': 0.3333333333333335, 'genre_Romance': 1.4615384615384617, 'genre_Sci-Fi': 0.1747685185185186, 'genre_Thriller': -0.013749999999999929, 'genre_War': 0.625, 'genre_Western': 0.75}")
    profile_dict = r.get_profile_as_dict(user_id=4)
    print(profile_dict)
