from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json


class CassRatingsCount:
    def __init__(self):
        self.keyspace = "ratings"
        self.table = "user_counts"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.create_keyspace()
        self.session.set_keyspace(keyspace=self.keyspace)
        self.session.row_factory = dict_factory

        self.create_table()


        self.list_of_all_genres = ['genre_action', 'genre_adventure',
                              'genre_animation', 'genre_children', 'genre_comedy', 'genre_crime', 'genre_documentary',
                              'genre_drama', 'genre_fantasy',
                              'genre_film_noir', 'genre_horror', 'genre_musical', 'genre_mystery', 'genre_romance',
                              'genre_sci_fi', 'genre_thriller',
                              'genre_war', 'genre_western']


    def create_keyspace(self):
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)

    def create_table(self):
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + self.table + """ (
        user_id double, genre_Action double, genre_Adventure double, 
        genre_Animation double, genre_Children double, genre_Comedy double, genre_Crime double, genre_Documentary double,
        genre_Drama double, genre_Fantasy double, genre_Film_Noir double, genre_Horror double, genre_Musical double,
        genre_Mystery double, genre_Romance double, genre_Sci_Fi double, genre_Thriller double, genre_War double, genre_Western double,
        PRIMARY KEY(user_id))
        """)

    def set_count_for_user(self, user_id, genre_count_dict):
        genre_count_json = json.dumps(genre_count_dict)
        genre_count_json = genre_count_json.lower()
        genre_count_json = genre_count_json.replace('-', '_')
        genre_count_json = genre_count_json.replace('null', '0')

        dict_of_arguments = json.loads(genre_count_json)
        list_of_input_keys = list(dict_of_arguments.keys())
        fixed_dict_of_arguments = {}
        fixed_dict_of_arguments['user_id'] = user_id



        for genre in self.list_of_all_genres:
            if genre in list_of_input_keys:
                fixed_dict_of_arguments[genre] = dict_of_arguments[genre]
            else:
                fixed_dict_of_arguments[genre] = 0

        list_of_arguments = list(fixed_dict_of_arguments.values())

        rating_push = self.session.prepare("""
        INSERT INTO """ + self.keyspace + """.""" + self.table + """ (user_id, genre_Action, genre_Adventure, 
        genre_Animation, genre_Children, genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, 
        genre_Film_Noir, genre_Horror, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Thriller, 
        genre_War, genre_Western)
        VALUES (?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?, ?, ? ,?)
        """)

        self.session.execute(rating_push, list_of_arguments)

        # reset session
        self.session = self.cluster.connect(keyspace=self.keyspace)
        self.session.row_factory = dict_factory

    def get_count_of_user_as_dict(self, user_id):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + " WHERE user_id=" + str(user_id) + ";")
        result_list = []
        for row in rows:
            result_list.append(row)
        try:
            return result_list[0]
        except IndexError:
            return dict.fromkeys(self.list_of_all_genres, 0)




    def get_data_table_as_list_of_dicts(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        result_list = []
        for row in rows:
            result_list.append(row)
        return result_list

    def delete_all(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")


if __name__ == "__main__":
    cass = CassRatingsCount()
    cass.create_table()

    genre_count_dict = '''{"user_id": 7556, "genre_Adventure": 13, "genre_comedy": 1, "genre_drama": 0,
     "genre_fantasy": 9, "genre_mystery": 0, "genre_Romance": 1, "genre_sci_fi": 33, "genre_thriller": 0,
     "genre_war": 0}'''
    cass.set_count_for_user(user_id=7556, genre_count_json=genre_count_dict)

    result = cass.get_data_table_as_list_of_dicts()
    print(result)
    # cass.clear_table()

