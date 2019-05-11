from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json
import pandas as pd
import uuid

class CassRatings:
    def __init__(self):
        self.keyspace = "ratings"
        self.table = "user_ratings"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.create_keyspace()
        self.session.set_keyspace(keyspace=self.keyspace)
        self.session.row_factory = dict_factory

        self.list_of_all_genres = ['genre_action', 'genre_adventure',
                                   'genre_animation', 'genre_children', 'genre_comedy', 'genre_crime',
                                   'genre_documentary',
                                   'genre_drama', 'genre_fantasy',
                                   'genre_film_noir', 'genre_horror', 'genre_musical', 'genre_mystery', 'genre_romance',
                                   'genre_sci_fi', 'genre_thriller',
                                   'genre_war', 'genre_western']
        self.create_table()

    def create_keyspace(self):
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)

    def create_table(self):
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + self.table + """ (
        id varchar, user_id double, movie_id double, rating double, genre_Action double, genre_Adventure double, 
        genre_Animation double, genre_Children double, genre_Comedy double, genre_Crime double, genre_Documentary double,
        genre_Drama double, genre_Fantasy double, genre_Film_Noir double, genre_Horror double, genre_Musical double,
        genre_Mystery double, genre_Romance double, genre_Sci_Fi double, genre_Thriller double, genre_War double, genre_Western double,
        PRIMARY KEY(id))
        """)

    def add_rating(self, rating_string_as_dict):
        rating_string_as_dict = rating_string_as_dict.lower()
        rating_string_as_dict = rating_string_as_dict.replace('-', '_')
        rating_string_as_dict = rating_string_as_dict.replace('null', '0')

        dict_of_arguments = json.loads(rating_string_as_dict)
        list_of_input_keys = list(dict_of_arguments.keys())

        fixed_dict_of_arguments = {}
        fixed_dict_of_arguments['id'] = str(uuid.uuid1())
        fixed_dict_of_arguments['user_id'] = dict_of_arguments['user_id']
        fixed_dict_of_arguments['movie_id'] = dict_of_arguments['movie_id']
        fixed_dict_of_arguments['rating'] = dict_of_arguments['rating']

        for genre in self.list_of_all_genres:
            if genre in list_of_input_keys:
                fixed_dict_of_arguments[genre] = dict_of_arguments[genre]
            else:
                fixed_dict_of_arguments[genre] = 0

        list_of_arguments = list(fixed_dict_of_arguments.values())
        rating_push = self.session.prepare("""
        INSERT INTO """ + self.keyspace + """.""" + self.table + """ (id, user_id, movie_id, rating, genre_Action, genre_Adventure, 
        genre_Animation, genre_Children, genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, 
        genre_Film_Noir, genre_Horror, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Thriller, 
        genre_War, genre_Western)
        VALUES (?, ?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ?)
        """)

        self.session.execute(rating_push, list_of_arguments)

        # reset session
        self.session = self.cluster.connect(keyspace=self.keyspace)
        self.session.row_factory = dict_factory

    def add_dataframe(self, dataframe):
        for i, row in dataframe.reset_index().iterrows():
            self.add_rating(row.to_json(orient='index'))

    def get_all_as_dataframe(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        ratings_list = []
        for row in rows:
            row = {key: row[key] for key in ['user_id', 'movie_id', 'rating'] + self.list_of_all_genres}
            ratings_list.append(json.dumps(row))

        df = pd.DataFrame()
        # columns = ['user_id'] + self.list_of_all_genres + ['movie_id', 'rating']
        # print(columns)

        # df.columns = columns
        for row in ratings_list:
            dictionary = json.loads(row)
            series = pd.Series(dictionary)
            df = df.append(series, ignore_index=True)
        df = df.fillna(value=0.0)

        return df

    def delete_all(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")


if __name__ == "__main__":
    cass = CassRatings()
    rating_dict = '''{"user_id": 3, "movie_id": 3, "rating": 1, "genre_Adventure": null, "genre_comedy": 1, "genre_drama": 0,
     "genre_fantasy": 0, "genre_mystery": 0, "genre_Romance": 1, "genre_sci_fi": 0, "genre_thriller": 0,
     "genre_war": 0}'''
    cass.add_rating(rating_dict)
    rating_dict = '''{"user_id":75.0,"movie_id":3.0,"rating":1.0,"genre_Action":0.0,"genre_Adventure":0.0,"genre_Animation":0.0,"genre_Children":0.0,"genre_Comedy":1.0,"genre_Crime":0.0,"genre_Documentary":0.0,"genre_Drama":0.0,"genre_Fantasy":0.0,"genre_Film-Noir":0.0,"genre_Horror":0.0,"genre_Musical":0.0,"genre_Mystery":0.0,"genre_Romance":1.0,"genre_Sci-Fi":0.0,"genre_Thriller":0.0,"genre_War":0.0,"genre_Western":0.0}'''

    cass.add_rating(rating_dict)

    result = cass.get_all_as_dataframe()
    print(result)
    # cass.clear_table()
