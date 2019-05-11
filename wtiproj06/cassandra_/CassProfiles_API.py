from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json


class CassProfiles:
    def __init__(self):
        self.keyspace = "ratings"
        self.table = "user_profiles"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.create_keyspace()
        self.session.set_keyspace(keyspace=self.keyspace)
        self.session.row_factory = dict_factory

        self.create_table()


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

    def set_profile(self, user_id, profile_dict):
        profile_json = json.dumps(profile_dict)
        profile_json = profile_json.lower()
        profile_json = profile_json.replace('-', '_')
        profile_json = profile_json.replace(' _', ' -')
        profile_json = profile_json.replace('null', '0')

        dict_of_arguments = json.loads(profile_json)
        list_of_input_keys = list(dict_of_arguments.keys())


        fixed_dict_of_arguments = {}
        fixed_dict_of_arguments['user_id'] = user_id

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
                fixed_dict_of_arguments[genre] = 0 #TODO wstawiac srednia

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

    def get_profile_as_dict(self, user_id):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + " WHERE user_id=" + str(user_id) + ";")
        list = []
        for row in rows:
            list.append(row)

        return list[0]

    def get_data_table_as_list_of_dicts(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        list = []
        for row in rows:
            list.append(row)
        return list

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")


if __name__ == "__main__":
    cass = CassProfiles()
    cass.create_table()

    rating_dict = '''{"user_id": 755, "genre_Adventure": 1.45, "genre_comedy": 1, "genre_drama": 0,
     "genre_fantasy": 4.56, "genre_mystery": 1.1111, "genre_Romance": 1, "genre_sci_fi": 0, "genre_thriller": 0,
     "genre_war": 0}'''
    cass.set_profile(rating_dict)
    cass.get_profile_as_dict(755)
    result = cass.get_data_table_as_list_of_dicts()
    print(result)
    # cass.clear_table()

