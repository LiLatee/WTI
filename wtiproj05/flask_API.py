from flask import Flask, request, Response

app = Flask(__name__)

from API import RedisApi

class FlaskAPI:

    def __init__(self):
        self.API = RedisApi()
        # self.API.fill_redis_from_data()
        # self.API.update_all_profiles()
        # self.API.update_all_avg_ratings_in_redis()

    @app.route('/')
    def home(self):
        return 'hello'

    @app.route('/ratings', methods=['POST'])
    def post_rating(self):
        return Response(self.API.post_rating(request.data), status=201, mimetype='application/json')


    @app.route('/ratings', methods=['GET'])
    def get_rating(self):
        return Response(self.API.get_all_ratings_as_json(), status=200, mimetype='application/json')

    @app.route('/ratings', methods=['DELETE'])
    def delete_rating(self):
        return Response(self.API.delete_all_ratings(), status=200, mimetype='application/json')

    @app.route('/avg-genre-ratings/all-users', methods=['GET'])
    def avg_genre_ratings_all_users(self):
        return Response(self.API.get_all_ratings_as_json(), status=200, mimetype='application/json')

    @app.route('/avg-genre-ratings/<int:user_id>', methods=['GET'])
    def avg_genre_ratings_user(self, user_id):
        return Response(self.API.get_user_avg_ratings_as_dict(user_id=user_id), status=200, mimetype='application/json')

if __name__ == '__main__':
    app.run()