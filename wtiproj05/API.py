import pandas as pd
import json
import numpy as np
import redis
from RedisProfiles_API import RedisProfiles
from RedisRatings_API import RedisRatings

class RedisApi:
    def __init__(self):
        self.redis_ratings = RedisRatings()
        self.redis_profiles = RedisProfiles()

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

    def get_user_profile_as_dict(self, user_id):
        avg_all, _ = self.get_all_avg_ratings_as_dict()
        avg_user = self.get_user_avg_ratings_as_dict(user_id)
        difference = dict.fromkeys(list(avg_all.keys()), 0)
        for x in avg_all:
            if np.isnan(avg_user[x]):
                difference[x] = 0
                continue
            difference[x] = avg_all[x] - avg_user[x]
        return difference

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



if __name__ == '__main__':
    r = RedisApi()
    # df = r.generate_ratings_as_datafram_from_data()
    # r.fill_redis_from_data()
    user_avg = r.get_user_avg_ratings_as_dict(75)
    print(user_avg)

    all_avg, _ = r.get_all_avg_ratings_as_dict()
    print(all_avg)

    user_profile = r.get_user_profile_as_dict(75)
    print(user_profile)

    all_ratings = r.get_all_ratings_as_json()
    print(all_ratings)