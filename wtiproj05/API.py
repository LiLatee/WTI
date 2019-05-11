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
        self.update_data_after_post(rating_dict=rating_dict)

        return "added"

    def delete_all_ratings(self):
        self.redis_ratings.delete_all()
        return "deleted"

    def set_all_avg_ratings_in_redis(self):
        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        avg_all_as_dict = self.data_processing.get_all_avg_ratings_as_dict(self.redis_ratings.get_all_as_dataframe())
        r.set(name='avg-all', value=json.dumps(avg_all_as_dict))

    def set_all_profiles_in_redis(self):
        avg_all = self.data_processing.get_all_avg_ratings_as_dict(self.redis_ratings.get_all_as_dataframe())
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

    def get_all_ratings_as_json(self):
        ratings = self.redis_ratings.get_all_as_dataframe()
        ratings_json = ratings.to_json(orient='index')
        return ratings_json

    def get_ids_of_all_users_as_set(self):
        #TODO chyba powinienem to brac redisa te ids
        ratings_json = self.get_all_ratings_as_json()
        ratings_dict = json.loads(ratings_json, )
        ratings_dataframe = pd.DataFrame(ratings_dict).T
        users_ids_set = set(ratings_dataframe['userID'].values)
        return users_ids_set

    def get_all_avg_ratings_as_dict(self):
        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        return json.loads(r.get('avg-all'))

    def get_user_avg_ratings_as_dict(self, user_id, avg_all_dict=None):
        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        if avg_all_dict is None:
            avg_all_dict = json.loads(r.get('avg-all'))
        user_profile = self.redis_profiles.get_profile_as_dict(user_id=user_id)
        user_avg = {}
        for genre in avg_all_dict:
            try:
                user_avg[genre] = avg_all_dict[genre] - user_profile[genre]
            except KeyError:
                user_avg[genre] = self.get_all_avg_ratings_as_dict()[genre]

        return user_avg

    def get_user_profile_as_dict(self, user_id):
        return self.redis_profiles.get_profile_as_dict(user_id)

    def get_count_of_ratings_of_all_genres_as_dict(self):
        return self.redis_count.get_count_of_all_users_as_dict()

    def get_ratings_count_of_all_genres_for_user_as_dict(self, user_id):
        return self.redis_count.get_count_of_user_as_dict(user_id=user_id)

    def update_data_after_post(self, rating_dict):
        self.update_count_of_ratings_after_post(rating_dict=rating_dict)
        old_avg_all_dict = self.update_avg_ratings_for_all_users_after_post(rating_dict=rating_dict)
        self.update_user_profile_after_post(rating_dict=rating_dict, old_avg_all_dict=old_avg_all_dict)

    def update_avg_ratings_for_all_users_after_post(self, rating_dict):
        rating_value = rating_dict['rating']
        genres_list_of_new_rating = list(rating_dict.keys())[3:-1]
        try:
            new_avg_all_dict = self.get_all_avg_ratings_as_dict()
        except TypeError:
            new_avg_all_dict = {}
            for genre in genres_list_of_new_rating:
                new_avg_all_dict[genre] = 0
        old_all_avg_dict = new_avg_all_dict.copy()

        ratings_count_of_all_genres_dict = self.get_count_of_ratings_of_all_genres_as_dict()
        for genre in genres_list_of_new_rating:
            if rating_dict[genre] == 1:
                count = ratings_count_of_all_genres_dict[genre] - 1
                old_avg_rating_for_all_users = new_avg_all_dict[genre]
                new_avg_all_dict[genre] = (count*old_avg_rating_for_all_users+rating_value)/(count+1)
                # if genre == 'genre_Comedy':
                #     print(count)
                #     print(old_avg_rating_for_all_users)
                #     print(rating_value)


        r = redis.StrictRedis(host='localhost', port=6381, db=0)
        r.set(name='avg-all', value=json.dumps(new_avg_all_dict))
        # print('update_avg_ratings_for_all_users_after_insert')
        # print(old_all_avg_dict)
        # print(new_avg_all_dict)
        return old_all_avg_dict

    def update_user_profile_after_post(self, rating_dict, old_avg_all_dict=None):
        genres_list_of_new_rating = list(rating_dict.keys())[3:-1]
        user_id = rating_dict['userID']
        rating_value = rating_dict['rating']
        old_avg_user_dict = self.get_user_avg_ratings_as_dict(user_id=user_id, avg_all_dict=old_avg_all_dict)
        new_avg_all_dict = self.get_all_avg_ratings_as_dict()

        new_avg_user_dict = self.get_user_avg_ratings_as_dict(user_id=user_id)
        counts_dict = self.redis_count.get_count_of_user_as_dict(user_id=user_id)
        for genre in genres_list_of_new_rating:
            if counts_dict[genre] == 0:
                new_avg_user_dict[genre] = self.get_all_avg_ratings_as_dict()[genre]
            else:
                new_avg_user_dict[genre] = (old_avg_user_dict[genre]*(counts_dict[genre]-1) + rating_value) / (counts_dict[genre]-1+1)


        new_user_profile_dict = self.get_user_profile_as_dict(user_id=user_id)
        for genre in genres_list_of_new_rating:
            new_user_profile_dict[genre] = new_avg_all_dict[genre] - new_avg_user_dict[genre]


        self.redis_profiles.set_profile(user_id=user_id, profile_dict=new_user_profile_dict)

    def update_count_of_ratings_after_post(self, rating_dict):
        user_id = rating_dict['userID']
        old_genre_count_of_user_dict = self.redis_count.get_count_of_user_as_dict(user_id=user_id)
        new_genre_count_of_user_dict = {}
        genres_list_of_new_rating = list(rating_dict.keys())[3:-1]
        for genre in genres_list_of_new_rating:
            if rating_dict[genre] == 1:
                try:
                    new_genre_count_of_user_dict[genre] = old_genre_count_of_user_dict[genre] + 1
                except KeyError:
                    new_genre_count_of_user_dict[genre] = 0
            else:
                try:
                    new_genre_count_of_user_dict[genre] = old_genre_count_of_user_dict[genre]
                except KeyError:
                    new_genre_count_of_user_dict[genre] = 0

        try:
            old_genre_count_of_all_users_dict = self.redis_count.get_count_of_all_users_as_dict()
        except TypeError:
            old_genre_count_of_all_users_dict = {}
            for genre in genres_list_of_new_rating:
                old_genre_count_of_all_users_dict[genre] = 0

        new_genre_count_of_all_users_dict = {}
        for genre in genres_list_of_new_rating:
            if rating_dict[genre] == 1:
                new_genre_count_of_all_users_dict[genre] = old_genre_count_of_all_users_dict[genre] + 1
            else:
                new_genre_count_of_all_users_dict[genre] = old_genre_count_of_all_users_dict[genre]

        self.redis_count.set_count_for_user(user_id=user_id, genre_count_dict=new_genre_count_of_user_dict)
        self.redis_count.set_count_of_all_users(genre_count_dict=new_genre_count_of_all_users_dict)


    # def update_avg_ratings_for_user_after_insert(self, rating_dict):
    #     user_id = rating_dict['userID']
    #     genres_list_of_new_rating = list(rating_dict.keys())[3:-1]
    #
    #     r = redis.StrictRedis(host='localhost', port=6381, db=0)
    #     avg_all_dict = json.loads(r.get(name='avg-all'))
    #     user_profile_dict = self.redis_profiles.get_profile_as_dict(user_id=user_id)
    #     avg_user_dict = dict.fromkeys(avg_all_dict.keys())
    #
    #     for genre in avg_all_dict:
    #         avg_user_dict[genre] = avg_all_dict[genre] - user_profile_dict[genre]
    #
    #     ratings_count_for_user_dict = self.get_ratings_count_of_all_genres_for_user_as_dict(user_id=user_id)
    #     rating_value = rating_dict['rating']
    #     new_avg_for_user_list = dict.fromkeys(avg_all_dict.keys())
    #     new_profile_dict = dict.fromkeys(avg_all_dict.keys())
    #
    #     for genre in genres_list_of_new_rating:
    #         count = ratings_count_for_user_dict[genre]
    #         avg_user_genre = avg_user_dict[genre]
    #         new_avg_for_user_list[genre] = (count*avg_user_genre+rating_value)/(count+1)
    #         new_profile_dict[genre] = avg_all_dict[genre] - avg_user_dict[genre]
    #
    #     self.redis_profiles.set_profile(user_id=user_id, profile_dict=new_profile_dict)





if __name__ == '__main__':
    r = RedisApi()
    r.fill_redis_from_csv()
    r.set_all_avg_ratings_in_redis()
    r.set_all_profiles_in_redis()
    r.set_all_count_of_ratings_in_redis()

    print(r.get_count_of_ratings_of_all_genres_as_dict())

    r.post_rating(rating='{"userID": 755,"movieID": 3,"rating": 1,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}')
    print(r.get_user_avg_ratings_as_dict(user_id=755))
    print(r.get_ratings_count_of_all_genres_for_user_as_dict(user_id=755))
    print(r.get_user_profile_as_dict(user_id=755))
    print(r.get_count_of_ratings_of_all_genres_as_dict())
    print(r.get_all_avg_ratings_as_dict())
    print(r.get_ids_of_all_users_as_set())
    print(r.get_all_ratings_as_json())
    r.delete_all_ratings()
    print(r.get_all_ratings_as_json())




    # print(r.get_all_avg_ratings_as_dict())
    # rating_dict = json.loads('{"userID": 75,"movieID": 3,"rating": 1,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}')
    # print(rating_dict)
    # r.update_data_after_insert_rating(rating_dict, 75)








    # r.update_avg_ratings_for_all_users_after_insert(js)
    # print(r.get_all_avg_ratings_as_dict())

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



# USER_PROFILE
# profile = avg_all - avg_user
# profile + avg_user = avg_all
# avg_user = avg_all - profile
#
# ===========================
# AVG
# old_avg: 3.5
# new_value: 3.0
# new_avg: (old_avg*counts + new_value)/(counts + 1)
#
# ===========================
# USER_PROFILE_UPDATE
# old_avg_all
# new_avg_all
# old_user_profile
#
# new_avg_user = old_avg_all - old_profile
# new_profile = new_avg_all - new_avg_user