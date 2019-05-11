import pandas as pd
import numpy as np


class DataProcessing:

    def get_merged_table_as_dataframe_from_csv(self):
        user_rated = pd.read_csv(filepath_or_buffer='user_ratedmovies.dat', sep='\t', nrows=100)
        movie_genres = pd.read_csv('movie_genres.dat', sep='\t')

        merged = user_rated.merge(movie_genres, on='movieID')
        merged.to_csv("merged.csv", sep='\t')

        ratings_one_hot = pd.concat([merged, pd.get_dummies(merged['genre'], prefix='genre')], axis=1)

        ratings_one_hot_grouped = ratings_one_hot.groupby(['userID', 'movieID', 'rating']).sum().drop(
            [col for col in ratings_one_hot.columns if 'date' in col], axis=1)

        genres_columns = list(ratings_one_hot_grouped.columns)
        return merged, genres_columns

    def generate_ratings_as_dataframe_from_csv(self):
        user_movie_genre_rating, _ = self.get_merged_table_as_dataframe_from_csv()
        # result = result[['userID', 'movieID', 'rating', 'genre']]
        ratings_one_hot = pd.concat(
            [user_movie_genre_rating, pd.get_dummies(user_movie_genre_rating['genre'], prefix='genre')], axis=1)

        ratings_one_hot_grouped = ratings_one_hot.groupby(['userID', 'movieID', 'rating']).sum().drop(
            [col for col in ratings_one_hot.columns if 'date' in col], axis=1)
        # ratings_one_hot_grouped.set_index(['userID','movieID'])
        ratings_one_hot_grouped.to_csv('ratings.csv', sep='\t')

        return ratings_one_hot_grouped

    def get_all_avg_ratings_as_dict(self, ratings_dataframe):
        # tworzymy slownik ze wszystkich gatunkow
        genre_columns = ratings_dataframe.iloc[:, :-3].columns
        avg_genre_ratings = dict.fromkeys(genre_columns, 0)
        # dla kazdego gatunku liczymy srednia ocen
        for x in avg_genre_ratings:
            avg_genre_ratings[x] = ratings_dataframe[ratings_dataframe[x] == 1.0].loc[:, 'rating'].mean()

        # ratingi pomniejszone o srednia ocene danego filmu
        merged, _ = self.get_merged_table_as_dataframe_from_csv()
        for x in avg_genre_ratings:
            merged.loc[merged['genre'] == x, 'rating'] = merged['rating'] - avg_genre_ratings[x]

        return avg_genre_ratings

    def get_user_avg_ratings_as_dict(self, ratings_dataframe, user_id):
        # tworzymy slownik ze wszystkich gatunkow
        genre_columns = ratings_dataframe.iloc[:, :-3].columns
        avg_genre_ratings = dict.fromkeys(genre_columns, 0)

        # dla kazdego gatunku liczymy srednia dla danego usera
        for x in avg_genre_ratings:
            avg_genre_ratings[x] = ratings_dataframe[(ratings_dataframe[x] == 1.0) & (ratings_dataframe['userID'] == user_id)].loc[:, 'rating'].mean()

        # dodajemy userID do slownika
        avg_genre_ratings['userID'] = user_id

        return avg_genre_ratings

    def get_user_profile_as_dict(self, ratings_dataframe, user_id):
        avg_all = self.get_all_avg_ratings_as_dict(ratings_dataframe)

        avg_user = self.get_user_avg_ratings_as_dict(ratings_dataframe=ratings_dataframe, user_id=user_id)
        profile_dict = dict.fromkeys(list(avg_all.keys()), 0)
        for x in avg_all:
            if np.isnan(avg_user[x]):
                profile_dict[x] = 0
                continue
            profile_dict[x] = avg_all[x] - avg_user[x]

        return  profile_dict