from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json


class CassRatings:
    def __init__(self):
        self.keyspace = "ratings"
        self.table = "avg_all"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.create_keyspace()
        self.session.set_keyspace(keyspace=self.keyspace)
        self.session.row_factory = dict_factory

    def create_keyspace(self):
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)

    def create_table(self):
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + self.table + """ (
        id int, genre_Action double, genre_Adventure double, 
        genre_Animation double, genre_Children double, genre_Comedy double, genre_Crime double, genre_Documentary double,
        genre_Drama double, genre_Fantasy double, genre_Film_Noir double, genre_Horror double, genre_Musical double,
        genre_Mystery double, genre_Romance double, genre_Sci_Fi double, genre_Thriller double, genre_War double, genre_Western double,
        PRIMARY KEY(id))
        """)

    def push_data_table(self, rating_string_as_dict):
        self.delete_table()
        self.create_table()
        rating_string_as_dict = rating_string_as_dict.lower()
        rating_string_as_dict = rating_string_as_dict.replace('-', '_')
        rating_string_as_dict = rating_string_as_dict.replace('null', '0')

        dict_of_arguments = json.loads(rating_string_as_dict)
        list_of_input_keys = list(dict_of_arguments.keys())
        fixed_dict_of_arguments = {}
        fixed_dict_of_arguments['id'] = 0


        list_af_all_genres = ['genre_action', 'genre_adventure',
                              'genre_animation', 'genre_children', 'genre_comedy', 'genre_crime', 'genre_documentary',
                              'genre_drama', 'genre_fantasy',
                              'genre_film_noir', 'genre_horror', 'genre_musical', 'genre_mystery', 'genre_romance',
                              'genre_sci_fi', 'genre_thriller',
                              'genre_war', 'genre_western']

        for genre in list_af_all_genres:
            if genre in list_of_input_keys:
                fixed_dict_of_arguments[genre] = dict_of_arguments[genre]
            else:
                fixed_dict_of_arguments[genre] = 0

        list_of_arguments = list(fixed_dict_of_arguments.values())

        rating_push = self.session.prepare("""
        INSERT INTO """ + self.keyspace + """.""" + self.table + """ (id, genre_Action, genre_Adventure, 
        genre_Animation, genre_Children, genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, 
        genre_Film_Noir, genre_Horror, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Thriller, 
        genre_War, genre_Western)
        VALUES (?, ?, ? ,? ,?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        self.session.execute(rating_push, list_of_arguments)

        # reset session
        self.session = self.cluster.connect(keyspace=self.keyspace)

    def get_data_table_as_list_of_dicts(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        print(rows)
        list = []
        for row in rows:
            list.append(row)
        return list

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")


if __name__ == "__main__":
    cass = CassRatings()
    cass.create_table()

    rating_dict = '''{"genre_Adventure": 2.33, "genre_comedy": 5.7856, "genre_drama": 0,
     "genre_fantasy": 0, "genre_mystery": 0, "genre_Romance": 1, "genre_sci_fi": 0, "genre_thriller": 0,
     "genre_war": 0}'''
    cass.push_data_table(rating_dict)

    result = cass.get_data_table_as_list_of_dicts()
    print(result)
    # cass.clear_table()

