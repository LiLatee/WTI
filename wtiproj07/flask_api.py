from flask import Flask, jsonify, abort, request, Response
from elasticsearch_simple_client import ElasticClient
import json

app = Flask(__name__)
es = ElasticClient()


# ------ Simple operations ------
@app.route("/user/document/<id>", methods=["GET"])
def get_user(id):
    try:
        index = request.args.get('index', default='users')
        result = es.get_movies_liked_by_user(id, index)
        return str(result)

    except:
        abort(404)

@app.route("/movie/document/<id>", methods=["GET"])
def get_movie(id):
    try:
        index = request.args.get('index', default='movies')
        result = es.get_users_that_like_movie(id, index)
        return str(result)
    except:
        abort(404)

# ------ Preselection ------
@app.route("/user/preselection/<id>", methods=["GET"])
def user_preselection(id):
    try:
        result = es.get_preselected_movies_for_user(int(id))
        result = {"moviesFound": result}
        return str(result)
    except:
        abort(404)

@app.route("/movie/preselection/<id>", methods=["GET"])
def movies_preselection(id):
    try:
        result = es.get_preselected_users_for_movie(int(id))
        result = {"usersFound": result}
        return str(result)
    except:
        abort(404)

# ------ Add/Update/Delete ------
@app.route("/user/document/<user_id>", methods=["POST"])
def add_user_document(user_id):
    try:
        movies_liked_by_user = json.loads(request.data)['ratings']
        es.add_user(int(user_id), movies_liked_by_user)
        return "Ok", 200
    except:
        abort(400)

@app.route("/movie/document/<movie_id>", methods=["POST"])
def add_movie_document(movie_id):
    try:
        users_that_like_movie = json.loads(request.data)['whoRated']
        es.add_movie(int(movie_id), users_that_like_movie)
        return "Ok", 200
    except:
        abort(400)

# @app.route("/user/document/<user_id>", methods=["PUT"])
# def update_user_document(user_id):
#     try:
#         new_fields = request.json
#         es.update_user(user_id, new_fields)
#         return "Ok", 200
#     except:
#         abort(400)
#
# @app.route("/movie/document/<movie_id>", methods=["PUT"])
# def update_movie_document(movie_id):
#     try:
#         new_fields = request.json
#         es.update_movie(movie_id, new_fields)
#         return "Ok", 200
#     except:
#         abort(400)


@app.route("/user/document/<user_id>", methods=["PUT"])
def update_user_document(user_id):
    try:
        new_movies = json.loads(request.data)['ratings']
        es.update_user_movies(int(user_id), new_movies)
        return "Ok", 200
    except:
        abort(400)

@app.route("/movie/document/<movie_id>", methods=["PUT"])
def update_movie_document(movie_id):
    try:
        new_users = json.loads(request.data)['whoRated']
        es.update_movie_users(int(movie_id), new_users)
        return "Ok", 200
    except:
        abort(400)

@app.route("/user/document/<user_id>", methods=["DELETE"])
def delete_user_document(user_id):
    try:
        es.delete_user(int(user_id))
        return "Ok", 200
    except:
        abort(400)

@app.route("/movie/document/<movie_id>", methods=["DELETE"])
def delete_movie_document(movie_id):
    try:
        es.delete_movie(int(movie_id))
        return "Ok", 200
    except:
        abort(400)

@app.route("/user/document/exists/<user_id>", methods=["GET"])
def exists_user_document(user_id):
    try:
        es.exists_user(int(user_id))
        return Response(str(es.exists_user(int(user_id))), status=200, mimetype='application/json')
    except:
        abort(400)

@app.route("/movie/document/exists/<movie_id>", methods=["GET"])
def exists_movie_document(movie_id):
    try:
        return Response(str(es.exists_movie(int(movie_id))), status=200, mimetype='application/json')
    except:
        abort(400)

if __name__ == '__main__':
    es.index_documents()
    app.run()