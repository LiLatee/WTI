import redis
import pandas as pd
import json


class RedisRatings:
    def __init__(self):
        self.list_name = 'ratings'
        self.redis = redis.StrictRedis(host='localhost', port=6381, db=0)


    def add_dataframe(self, dataframe):
        for i, row in dataframe.reset_index().iterrows():
            self.redis.rpush('ratings', row.to_json(orient="index"))

    def add_rating(self, rating):
        self.redis.rpush(self.list_name, rating)

    def delete_all(self):
        self.redis.delete(self.list_name)

    def get_all_as_dataframe(self):
        ratings_list = self.redis.lrange(self.list_name, 0, -1)
        df = pd.DataFrame()
        for row in ratings_list:
            dictionary = json.loads(row)
            series = pd.Series(dictionary)
            df = df.append(series, ignore_index=True)
        df = df.fillna(value=0.0)
        # df = df.set_index(['userID', 'movieID', 'rating'])
        return df


if __name__ == '__main__':
    r = RedisRatings()
    r.add_rating('{"userID": 755,"movieID": 3,"rating": 1,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}')
    r.delete_all()
    result = r.get_all_as_dataframe()
    print(result)