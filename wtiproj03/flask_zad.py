from flask import Flask, request, Response

app = Flask(__name__)

import zad1



@app.route('/')
def home():
    return 'hello'

@app.route('/ratings', methods=['POST'])
def postRating():
    return Response(zad1.add(request.data), status=201, mimetype='application/json')


@app.route('/ratings', methods=['GET'])
def getRating():
    return Response(zad1.getRating(), status=200, mimetype='application/json')

@app.route('/ratings', methods=['DELETE'])
def deleteRating():
    return Response(zad1.delete(), status=200, mimetype='application/json')

@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def avg_genre_ratings_all_users():
    return Response(zad1.avg_genre_ratings_all_users(), status=200, mimetype='application/json')

@app.route('/avg-genre-ratings/<int:user_id>', methods=['GET'])
def avg_genre_ratings_user(user_id):
    return Response(zad1.avg_genre_rating_user(user_id), status=200, mimetype='application/json')

if __name__ == '__main__':
    zad1.merge_zad1()
    zad1.ratings_zad2()
    app.run()