import pandas as pd
import json
import numpy as np
import redis

from RedisProfiles_API import RedisProfiles
from RedisRatings_API import RedisRatings
from RedisRatingsCount_API import RedisRatingsCount
from data_processing import DataProcessing


class RedisApi:
    def __init__(self):
        self.redis_ratings = RedisRatings()
        self.redis_profiles = RedisProfiles()
        self.redis_count = RedisRatingsCount()
        self.data_processing = DataProcessing()

    def fill_redis_from_csv(self):
        self.redis_ratings.add_dataframe(self.data_processing.generate_ratings_as_dataframe_from_csv())

    def post_rating(self, rating):
        self.redis_ratings.add_rating(rating)
        rating_dict = json.loads(rating)
        rating_dataframe = pd.Series(data=rating_dict)
        rating_dataframe = rating_dataframe.fillna(value=0)
        self.update_user_profile_after_insert_rating(user_id=rating_dataframe['userID'])
        self.update_avg_ratings_for_all_users_after_insert(rating_dict)
        self.update_avg_ratings_for_user_after_insert(rating_dict)

        return "added"

    def delete_all_ratings(self):
        self.redis_ratings.delete_all()
        return "deleted"

    def set_all_avg_ratings_in_redis(self):
        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        avg_all_as_dict = self.data_processing.get_avg_all_ratings_as_dict(self.redis_ratings.get_all_as_dataframe())
        r.set(name='avg-all', value=json.dumps(avg_all_as_dict))

    def set_all_profiles_in_redis(self):
        avg_all = self.data_processing.get_avg_all_ratings_as_dict(self.redis_ratings.get_all_as_dataframe())
        users_ids_set = self.get_ids_of_all_users_as_set()

        for user_id in users_ids_set:
            avg_user = self.data_processing.get_user_avg_ratings_as_dict(self.redis_ratings.get_all_as_dataframe(), user_id)
            profile_dict = dict.fromkeys(list(avg_all.keys()), 0)
            for x in avg_all:
                if np.isnan(avg_user[x]):
                    profile_dict[x] = 0
                    continue
                profile_dict[x] = avg_all[x] - avg_user[x]

            self.redis_profiles.set_profile(user_id=user_id, profile_dict=profile_dict)

    def set_all_count_of_ratings_in_redis(self):
        ratings_dataframe = self.redis_ratings.get_all_as_dataframe()
        ratings_dataframe = ratings_dataframe.set_index(keys=['userID', 'movieID', 'rating'])
        list_of_genres = ratings_dataframe.columns
        set_of_users_ids = self.get_ids_of_all_users_as_set()
        count_of_ratings_dict = dict.fromkeys(list_of_genres)

        # set ratings count of each user
        for id in set_of_users_ids:
            for genre in list_of_genres:
                count_of_ratings_dict[genre] = 0  # None --> 0
                count_of_zeros_and_ones = ratings_dataframe.loc[id][genre].value_counts()
                if count_of_zeros_and_ones.get(1.0) is not None:
                    count_of_ratings_dict[genre] = count_of_zeros_and_ones.get(1.0).item()
            self.redis_count.set_count_for_user(user_id=id, genre_count_dict=count_of_ratings_dict)

        # set ratings count of all users
        for genre in list_of_genres:
            count_of_ratings_dict[genre] = 0  # None --> 0
            count_of_zeros_and_ones = ratings_dataframe[genre].value_counts()
            if count_of_zeros_and_ones.get(1.0) is not None:
                count_of_ratings_dict[genre] = count_of_zeros_and_ones.get(1.0).item()
        self.redis_count.set_count_of_all_users(genre_count_dict=count_of_ratings_dict)

    def get_ids_of_all_users_as_set(self):
        #TODO chyba powinienem to brac redisa te ids
        ratings = self.redis_ratings.get_all_as_dataframe()
        ratings_json = ratings.to_json(orient='index')

        ratings_dict = json.loads(ratings_json, )
        ratings_dataframe = pd.DataFrame(ratings_dict).T
        users_ids_set = set(ratings_dataframe['userID'].values)
        return users_ids_set

###################


    def get_all_avg_ratings_as_dict(self):
        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        return json.loads(r.get('avg-all'))

    def get_user_profile_as_dict(self, user_id):
        return self.redis_profiles.get_profile_as_dict(user_id)


    def get_ratings_count_of_all_genres_as_dict(self):
        return self.redis_count.get_count_of_all_users_as_dict()

    def get_ratings_count_of_all_genres_for_user_as_dict(self, user_id):
        return self.redis_count.get_count_of_user_as_dict(user_id=user_id)

    def update_avg_ratings_for_all_users_after_insert(self, rating_dict):
        rating = rating_dict['rating']
        genres_list_of_new_rating = list(rating_dict.keys())[3:-1]

        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        avg_all_dict = json.loads(r.get(name='avg-all'))
        ratings_count_of_all_genres_dict = self.get_ratings_count_of_all_genres_as_dict()
        for genre in genres_list_of_new_rating:
            count = ratings_count_of_all_genres_dict[genre]
            avg_rating_for_all_users = avg_all_dict[genre]
            avg_all_dict[genre] = (count*avg_rating_for_all_users+rating)/(count+1)

        r.set(name='avg-all', value=json.dumps(avg_all_dict))

    def update_avg_ratings_for_user_after_insert(self, rating_dict):
        user_id = rating_dict['userID']
        genres_list_of_new_rating = list(rating_dict.keys())[3:-1]

        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        avg_all_dict = json.loads(r.get(name='avg-all'))
        user_profile_dict = self.redis_profiles.get_profile_as_dict(user_id=user_id)
        avg_user_dict = dict.fromkeys(avg_all_dict.keys())

        for genre in avg_all_dict:
            avg_user_dict[genre] = avg_all_dict[genre] - user_profile_dict[genre]

        ratings_count_for_user_dict = self.get_ratings_count_of_all_genres_for_user_as_dict(user_id=user_id)
        rating_value = rating_dict['rating']
        new_avg_for_user_list = dict.fromkeys(avg_all_dict.keys())
        new_profile_dict = dict.fromkeys(avg_all_dict.keys())

        for genre in genres_list_of_new_rating:
            count = ratings_count_for_user_dict[genre]
            avg_user_genre = avg_user_dict[genre]
            new_avg_for_user_list[genre] = (count*avg_user_genre+rating_value)/(count+1)
            new_profile_dict[genre] = avg_all_dict[genre] - avg_user_dict[genre]

        self.redis_profiles.set_profile(user_id=user_id, profile_dict=new_profile_dict)

    def update_user_profile_after_insert_rating(self, user_id):
        avg_all = self.compute_avg_all_ratings_as_dict()
        avg_user = self.get_user_avg_ratings_as_dict(user_id=user_id)
        profile_dict = dict.fromkeys(list(avg_all.keys()), 0)
        for x in avg_all:
            if np.isnan(avg_user[x]):
                profile_dict[x] = 0
                continue
            profile_dict[x] = avg_all[x] - avg_user[x]

        self.redis_profiles.set_profile(user_id=user_id, profile_dict=profile_dict)










if __name__ == '__main__':
    r = RedisApi()
    r.fill_redis_from_csv()
    r.set_all_avg_ratings_in_redis()
    r.set_all_profiles_in_redis()
    r.set_all_count_of_ratings_in_redis()
    # r.post_rating('{"userID": 755,"movieID": 3,"rating": 1,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}')
    # r.update_avg_ratings_for_user_in_redis(None, None, 75)


    # df = r.generate_ratings_as_dataframe_from_data()
    # user_avg = r.get_user_avg_ratings_as_dict(75)
    # print(user_avg)
    #
    # all_avg, _ = r.get_all_avg_ratings_as_dict()
    # print(all_avg)
    #
    # user_profile = r.get_user_profile_as_dict(75)
    # print(user_profile)
    #
    # all_ratings = r.get_all_ratings_as_json()
    # print(all_ratings)

