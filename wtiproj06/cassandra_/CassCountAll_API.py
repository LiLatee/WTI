from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json


class CassCountAll:
    def __init__(self):
        self.keyspace = "ratings"
        self.table = "count_all"
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
        id double, genre_Action double, genre_Adventure double, 
        genre_Animation double, genre_Children double, genre_Comedy double, genre_Crime double, genre_Documentary double,
        genre_Drama double, genre_Fantasy double, genre_Film_Noir double, genre_Horror double, genre_Musical double,
        genre_Mystery double, genre_Romance double, genre_Sci_Fi double, genre_Thriller double, genre_War double, genre_Western double,
        PRIMARY KEY(id))
        """)

    def set_count_of_all_users(self, genre_count_dict):
        self.clear_table()
        genre_count_json = json.dumps(genre_count_dict)
        genre_count_json = genre_count_json.lower()
        genre_count_json = genre_count_json.replace('-', '_')
        genre_count_json = genre_count_json.replace('null', '0')

        dict_of_arguments = json.loads(genre_count_json)
        list_of_input_keys = list(dict_of_arguments.keys())
        fixed_dict_of_arguments = {}
        fixed_dict_of_arguments['id'] = 0

        for genre in self.list_of_all_genres:
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
        self.session.row_factory = dict_factory

    def get_count_of_all_users_as_dict(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        list = []
        for row in rows:
            row = {key: row[key] for key in self.list_of_all_genres} # remove id key form result dict
            list.append(row)
        return list[0]

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")


if __name__ == "__main__":
    cass = CassCountAll()
    cass.create_table()

    genre_count_dict = '''{"genre_Adventure": 78, "genre_comedy": 100, "genre_drama": 0,
     "genre_fantasy": 0, "genre_mystery": 0, "genre_Romance": 1, "genre_sci_fi": 0, "genre_thriller": 0,
     "genre_war": 0}'''
    cass.set_count_of_all_users(genre_count_json=genre_count_dict)

    result = cass.get_count_of_all_users_as_dict()
    print(result)
    # cass.clear_table()
