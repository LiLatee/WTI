import os.path

import cherrypy

from API import API



class Ratings(object):
    def __init__(self):
        self.API = API()
        # self.API.fill_redis_from_csv()
        # self.API.set_all_profiles_in_redis()
        # self.API.set_all_avg_ratings_in_redis()
        # self.API.set_all_count_of_ratings_in_redis()

    @cherrypy.expose
    def index(self):
        return open('index.html')

    @cherrypy.expose
    def avg_genre_ratings_user(self, user_id):
        return str(self.API.get_user_avg_ratings_as_dict(int(user_id)))

    @cherrypy.expose
    def user_profile(self, user_id):
        return str(self.API.get_user_profile_as_dict(int(user_id)))

    @cherrypy.expose
    def avg_genre_ratings_all(self):
        avg = self.API.get_all_avg_ratings_as_dict()
        return str(avg)

    # @cherrypy.expose
    # def ratings(self):
    #     return str(self.API.get_all_ratings_as_json())

@cherrypy.expose
class RatingsWebService(object):
    def __init__(self):
        self.API = API()

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return str(self.API.get_all_ratings_as_json())

    def POST(self, data):
        data = data.lower()
        data = data.replace('-', '_')
        return self.API.post_rating(rating=data)

    def DELETE(self):
        return self.API.delete_all_ratings()

if __name__ == '__main__':
    conf = {
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd())
        },
        '/ratings': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        },
        '/avg_genre_ratings_all': {
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        },
        '/avg_genre_ratings_user': {
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        },
        '/user_profile': {
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        },
        # '/ratings': {
        #     'tools.response_headers.on': True,
        #     'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        # },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        }
    }

    webapp = Ratings()
    webapp.ratings = RatingsWebService()

    cherrypy.server.socket_port = 9898
    cherrypy.quickstart(webapp, '/', conf)




















# class StringGenerator(object):
#     @cherrypy.expose
#     def ratings(self):
#         return """
#         <html>
#           <body>
#         """ + zad1.getRating() + """
#         <form method="get" action="avg_genre_ratings_user">
#               <button type="submit">Pokaz oceny uzytkownika o danym id</button>
#               <input type="text" value="" name="userID" />
#             </form>
#               <form method="get" action="avg_genre_ratings_all_users">
#               <button type="submit">Pokaz srednie oceny wszystkich</button>
#             </form>
#           </body>
#         </html>"""
#
#     @cherrypy.expose
#     def avg_genre_ratings_user(self, userID=75):
#         return zad1.avg_genre_rating_user(int(userID))
#
#     @cherrypy.expose
#     def avg_genre_ratings_all_users(self):
#         return zad1.avg_genre_ratings_all_users()
#
#     @cherrypy.expose
#     def postRating(self):
#         return zad1.add()
#
#
# if __name__ == '__main__':
#     conf = {
#         '/': {
#             'tools.sessions.on': True,
#             'tools.staticdir.root': os.path.abspath(os.getcwd())
#         },
#         '/static': {
#             'tools.staticdir.on': True,
#             'tools.staticdir.dir': './public'
#         }
#     }
#     cherrypy.quickstart(StringGenerator(), '/', conf)
