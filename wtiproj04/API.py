import pandas as pd
import json
import math
import numpy as np

def zad1():
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


def zad2(df):
    return df.to_dict('records')

def zad3(ld):
    return pd.DataFrame.from_dict(ld)

def zad5():
    ratings = pd.read_csv('ratings.csv', sep='\t')
    # tworzymy slownik ze wszystkich gatunkow
    genre_columns = ratings.iloc[:, 3:].columns
    avg_genre_ratings = dict.fromkeys(genre_columns, 0)
    # dla kazdego gatunku liczymy srednia ocen
    for x in avg_genre_ratings:
        avg_genre_ratings[x] = ratings[ratings[x] == 1.0].loc[:, 'rating'].mean()

    # ratingi pomniejszone o srednia ocene danego filmu
    merged = pd.read_csv('merged.csv', sep='\t')
    for x in avg_genre_ratings:
        merged.loc['genre_' + merged['genre'] == x, 'rating'] = merged['rating'] - avg_genre_ratings[x]

    print(merged[['movieID', 'rating','genre']])
    return avg_genre_ratings, merged


def zad6(user_id):
    ratings = pd.read_csv('ratings.csv', sep='\t')
    # tworzymy slownik ze wszystkich gatunkow
    genre_columns = ratings.iloc[:, 3:].columns
    avg_genre_ratings = dict.fromkeys(genre_columns, 0)

    # dla kazdego gatunku liczymy srednia dla danego usera
    for x in avg_genre_ratings:
        avg_genre_ratings[x] = ratings[(ratings[x] == 1.0) & (ratings['userID'] == user_id)].loc[:, 'rating'].mean()

    # dodajemy userID do slownika
    avg_genre_ratings['userID'] = user_id

    return avg_genre_ratings


def zad7(user_id):
    avg_all, _ = zad5()
    avg_user = zad6(user_id)
    print(avg_all)
    print(avg_user)
    difference = dict.fromkeys(list(avg_all.keys()), 0)
    for x in avg_all:
        if np.isnan(avg_user[x]):
            difference[x] = 0
            continue
        difference[x] = avg_all[x] - avg_user[x]
    return difference



def ratings():
    user_movie_genre_rating, _ = zad1()
    # result = result[['userID', 'movieID', 'rating', 'genre']]
    ratings_one_hot = pd.concat([user_movie_genre_rating, pd.get_dummies(user_movie_genre_rating['genre'], prefix='genre')], axis=1)

    ratings_one_hot_grouped = ratings_one_hot.groupby(['userID', 'movieID', 'rating']).sum().drop(
        [col for col in ratings_one_hot.columns if 'date' in col], axis=1)
    # ratings_one_hot_grouped.set_index(['userID','movieID'])
    ratings_one_hot_grouped.to_csv('ratings.csv', sep='\t')
    return ratings_one_hot_grouped


def getRating():
    ratings = pd.read_csv('ratings.csv', sep='\t')
    return ratings.to_json(orient='index')

def add(data):
    ratings = pd.read_csv('ratings.csv', sep='\t')

    df = pd.Series(json.loads(data)).fillna(0)
    ratings = ratings.append(df, ignore_index=True).fillna(0)
    ratings.to_csv('ratings.csv', sep='\t', index=False)
    return 'added'

def delete():
    ratings = pd.read_csv('ratings.csv', sep='\t')
    ratings = ratings.iloc[-1:0]
    ratings.to_csv('ratings.csv', sep='\t')
    return "deleted"