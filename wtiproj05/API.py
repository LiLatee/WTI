import pandas as pd
import json
import numpy as np
from RedisProfiles_API import RedisProfiles
from RedisRatings_API import RedisRatings
from RedisRatingsCount_API import RedisRatingsCount


class RedisApi:
    def __init__(self):
        self.redis_ratings = RedisRatings()
        self.redis_profiles = RedisProfiles()
        self.redis_count = RedisRatingsCount()


    def get_merged_table_as_dataframe_from_data(self):
        # zad1
        user_rated = pd.read_csv('user_ratedmovies.dat', sep='\t', nrows=100)
        movie_genres = pd.read_csv('movie_genres.dat', sep='\t')
        merged = user_rated.merge(movie_genres, on='movieID')
        merged.to_csv("merged.csv", sep='\t')

        ratings_one_hot = pd.concat([merged, pd.get_dummies(merged['genre'], prefix='genre')], axis=1)

        ratings_one_hot_grouped = ratings_one_hot.groupby(['userID', 'movieID', 'rating']).sum().drop(
            [col for col in ratings_one_hot.columns if 'date' in col], axis=1)

        genres_columns = list(ratings_one_hot_grouped.columns)

        return merged, genres_columns

    def generate_ratings_as_datafram_from_data(self):
        user_movie_genre_rating, _ = self.get_merged_table_as_dataframe_from_data()
        # result = result[['userID', 'movieID', 'rating', 'genre']]
        ratings_one_hot = pd.concat(
            [user_movie_genre_rating, pd.get_dummies(user_movie_genre_rating['genre'], prefix='genre')], axis=1)

        ratings_one_hot_grouped = ratings_one_hot.groupby(['userID', 'movieID', 'rating']).sum().drop(
            [col for col in ratings_one_hot.columns if 'date' in col], axis=1)
        # ratings_one_hot_grouped.set_index(['userID','movieID'])
        ratings_one_hot_grouped.to_csv('ratings.csv', sep='\t')

        return ratings_one_hot_grouped

    def get_all_avg_ratings_as_dict(self):
        ratings = self.redis_ratings.get_all_as_dataframe()
        # tworzymy slownik ze wszystkich gatunkow
        genre_columns = ratings.iloc[:, :-3].columns
        avg_genre_ratings = dict.fromkeys(genre_columns, 0)
        # dla kazdego gatunku liczymy srednia ocen
        for x in avg_genre_ratings:
            avg_genre_ratings[x] = ratings[ratings[x] == 1.0].loc[:, 'rating'].mean()

        # ratingi pomniejszone o srednia ocene danego filmu
        merged, _ = self.get_merged_table_as_dataframe_from_data()
        for x in avg_genre_ratings:
            merged.loc[merged['genre'] == x, 'rating'] = merged['rating'] - avg_genre_ratings[x]

        return avg_genre_ratings, merged

    def get_user_avg_ratings_as_dict(self, user_id):
        ratings = self.redis_ratings.get_all_as_dataframe()
        # tworzymy slownik ze wszystkich gatunkow
        genre_columns = ratings.iloc[:, :-3].columns
        avg_genre_ratings = dict.fromkeys(genre_columns, 0)

        # dla kazdego gatunku liczymy srednia dla danego usera
        for x in avg_genre_ratings:
            avg_genre_ratings[x] = ratings[(ratings[x] == 1.0) & (ratings['userID'] == user_id)].loc[:, 'rating'].mean()

        # dodajemy userID do slownika
        avg_genre_ratings['userID'] = user_id

        return avg_genre_ratings

    def update_profiles(self):
        avg_all, _ = self.get_all_avg_ratings_as_dict()
        users_ids_set = self.get_ids_of_all_users_as_set()

        for id in users_ids_set:
            avg_user = self.get_user_avg_ratings_as_dict(id)
            profile_dict = dict.fromkeys(list(avg_all.keys()), 0)
            for x in avg_all:
                if np.isnan(avg_user[x]):
                    profile_dict[x] = 0
                    continue
                profile_dict[x] = avg_all[x] - avg_user[x]

            self.redis_profiles.add_profile(id, profile_dict)

    def get_ids_of_all_users_as_set(self):
        ratings_json = self.get_all_ratings_as_json()
        ratings_dict = json.loads(ratings_json, )
        ratings_dataframe = pd.DataFrame(ratings_dict).T
        users_ids_set = set(ratings_dataframe['userID'].values)
        return users_ids_set

    def get_user_profile_as_dict(self, user_id):
        return self.redis_profiles.get_profile(user_id)

    # def get_user_profile_as_dict(self, user_id):
    #     avg_all, _ = self.get_all_avg_ratings_as_dict()
    #     avg_user = self.get_user_avg_ratings_as_dict(user_id)
    #     difference = dict.fromkeys(list(avg_all.keys()), 0)
    #     for x in avg_all:
    #         if np.isnan(avg_user[x]):
    #             difference[x] = 0
    #             continue
    #         difference[x] = avg_all[x] - avg_user[x]
    #     return difference

    def get_all_ratings_as_json(self):
        ratings = self.redis_ratings.get_all_as_dataframe()
        return ratings.to_json(orient='index')

    def post_rating(self, rating):
        self.redis_ratings.add_rating(rating)
        return "added"

    def delete_all_ratings(self):
        self.redis_ratings.delete_all()
        return "deleted"

    def fill_redis_from_data(self):
        self.redis_ratings.add_dataframe(self.generate_ratings_as_datafram_from_data())

    def update_count_of_ratings(self):
        ratings_dataframe = self.redis_ratings.get_all_as_dataframe()
        ratings_dataframe = ratings_dataframe.set_index(keys=['userID', 'movieID', 'rating'])
        list_of_genres = ratings_dataframe.columns
        set_of_users_ids = self.get_ids_of_all_users_as_set()
        count_of_ratings_dict = dict.fromkeys(list_of_genres)
        for id in set_of_users_ids:
            for genre in list_of_genres:
                count_of_zeros_and_ones = ratings_dataframe.loc[id][genre].value_counts()
                count_of_ratings_dict[genre] = count_of_zeros_and_ones.get(1.0)
                print(count_of_ratings_dict)
                # self.redis_count.add_count(id, pd.DataFrame.from_dict(count_of_ratings_dict).to_json())


if __name__ == '__main__':
    r = RedisApi()
    # df = r.generate_ratings_as_datafram_from_data()
    # r.fill_redis_from_data()
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
    r.update_profiles()
    r = RedisApi()
    r.update_count_of_ratings()