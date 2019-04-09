import os, os.path

import cherrypy

import zad1



class Ratings(object):
    def __init__(self):
        zad1.merge_zad1()
        zad1.ratings_zad2()

    @cherrypy.expose
    def index(self):
        return open('index.html')

    @cherrypy.expose
    def avg_genre_ratings_user(self, userID):
        return str(zad1.avg_genre_rating_user(int(userID)))

    @cherrypy.expose
    def avg_genre_ratings_all(self):
        return str(zad1.avg_genre_ratings_all_users())

@cherrypy.expose
class RatingsWebService(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return str(zad1.getRating())

    def POST(self, data):
        return zad1.add(data)

    def DELETE(self):
        return zad1.delete()

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
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        }
    }

    webapp = Ratings()
    webapp.ratings = RatingsWebService()


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
