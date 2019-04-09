import pandas as pd
import json
import math


def merge_zad1():
    # zad1
    user_rated = pd.read_csv('user_ratedmovies.dat', sep='\t', nrows=100)
    movie_genres = pd.read_csv('movie_genres.dat', sep='\t')
    result = user_rated.merge(movie_genres, on='movieID')
    result.to_csv("merged.csv", sep='\t')
    return result

def ratings_zad2():
    user_movie_genre_rating = merge_zad1()
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


def avg_genre_ratings_all_users():
    ratings = pd.read_csv('ratings.csv', sep='\t')
    # tworzymy slownik ze wszystkich gatunkow
    genre_columns = ratings.iloc[:, 3:].columns
    avg_genre_ratings = dict.fromkeys(genre_columns, 0)

    # dla kazdego gatunku liczymy srednia ocen
    for x in avg_genre_ratings:
        avg_genre_ratings[x] = ratings[ratings[x] == 1.0].loc[:, 'rating'].mean()
    # # ustawiamy zera zamiast nan
    # for x in avg_genre_ratings:
    #     if math.isnan(avg_genre_ratings[x]) :
    #         avg_genre_ratings[x] = 0

    return json.dumps(avg_genre_ratings);

def avg_genre_rating_user(user_id):
    ratings = pd.read_csv('ratings.csv', sep='\t')
    # tworzymy slownik ze wszystkich gatunkow
    genre_columns = ratings.iloc[:, 3:].columns
    avg_genre_ratings = dict.fromkeys(genre_columns, 0)

    # dla kazdego gatunku liczymy srednia dla danego usera
    for x in avg_genre_ratings:
        avg_genre_ratings[x] = ratings[(ratings[x] == 1.0) & (ratings['userID'] == user_id)].loc[:,'rating'].mean()

    # dodajemy userID do slownika
    avg_genre_ratings['userID'] = user_id
    # # zastepujemy nan zerami
    # for x in avg_genre_ratings:
    #     if math.isnan(avg_genre_ratings[x]):
    #         avg_genre_ratings[x] = 0

    return json.dumps(avg_genre_ratings)




def avg_genre_ratings_all_users2():
    merged = pd.read_csv('merged.csv', sep='\t')
    # tworzymy slownik ze wszystkich gatunkow
    genre_columns = merged['genre']
    avg_genre_ratings = dict.fromkeys(genre_columns, 0)

    # dla kazdego gatunku liczymy srednia ocen
    for x in avg_genre_ratings:
        avg_genre_ratings[x] = merged.loc[merged['genre'] == x]['rating'].mean()
    # ustawiamy zera zamiast nan
    for x in avg_genre_ratings:
        if math.isnan(avg_genre_ratings[x]) :
            avg_genre_ratings[x] = 0

    return json.dumps(avg_genre_ratings);

def avg_genre_rating_user2(user_id):
    merged = pd.read_csv('merged.csv', sep='\t')
    # tworzymy slownik ze wszystkich gatunkow
    genre_columns = merged['genre']
    avg_genre_ratings = dict.fromkeys(genre_columns, 0)

    # dla kazdego gatunku liczymy srednia dla danego usera
    for x in avg_genre_ratings:
        avg_genre_ratings[x] = merged["rating"][(merged['genre'] == x) & (merged['userID'] == user_id)].mean()


    # dodajemy userID do slownika
    avg_genre_ratings['userID'] = user_id
    # zastepujemy nan zerami
    for x in avg_genre_ratings:
        if math.isnan(avg_genre_ratings[x]):
            avg_genre_ratings[x] = 0

    return json.dumps(avg_genre_ratings)


# if __name__ == '__main__':
#     generate_zad2()
#     w = '{"userID": 759, "movieID": 3,"rating": 1,"genre-Action": null,"genre-Adventure": null,"genre-Animation": null,"genre-Children": null,"genre-Comedy": 1,"genre-Crime": null,"genre-Documentary": null,"genre-Drama": null,"genre-Fantasy": null,"genre-Horror": null,"genre-IMAX": null,"genre-Mystery": null,"genre-Romance": 1,"genre-Sci-Fi": null,"genre-Thriller": null,"genre-War": null }'
#     add(w)
#     print(user_movie_genres)




# def join():
#     # zad1
#     user = pd.read_csv('user_ratedmovies.dat', sep='\t', nrows=1000)
#     movie = pd.read_csv('movie_genres.dat', sep='\t', nrows=1000)
#     result = user.join(movie.set_index('movieID'), on='movieID')
#     result = result[['userID', 'movieID', 'rating', 'genre']]
#
#     # zad2
#     unique_user_movie = result[['userID','movieID']].drop_duplicates()
#
#     columns = movie.genre.unique()
#     columns.sort()
#     for x in range(len(columns)):
#         columns[x] = 'genre-' + columns[x]
#
#     columns = np.insert(columns, 0, "rating")
#     columns = np.insert(columns, 0, "movieID")
#     columns = np.insert(columns, 0, "userID")
#
#     user_movie_genres = pd.DataFrame(unique_user_movie, columns=columns).set_index(['userID', 'movieID'])
#
#     for x in result.iterrows():
#         if ('genre-' + str(x[1]['genre'])) in columns:
#             user_movie_genres.at[(x[1]['userID'], x[1]['movieID']), 'genre-'+x[1]['genre']] = 1.0
#             user_movie_genres.at[(x[1]['userID'], x[1]['movieID']), 'rating'] = x[1]['rating']
#
#
#     return user_movie_genres.to_json(orient='index')