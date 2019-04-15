from flask import Flask, request, Response


from API import RedisApi
app = Flask(__name__)

API = RedisApi()
API.fill_redis_from_csv()
API.set_all_profiles()
API.set_avg_all_ratings_in_redis()


@app.route('/')
def home():
    return 'hello'

@app.route('/ratings', methods=['POST'])
def post_rating():
    return Response(API.post_rating(request.data), status=201, mimetype='application/json')


@app.route('/ratings', methods=['GET'])
def get_all_ratings():
    print("get_all_ratings")
    return Response(API.get_all_ratings_as_json(), status=200, mimetype='application/json')


@app.route('/ratings', methods=['DELETE'])
def delete_all_ratings():
    return Response(API.delete_all_ratings(), status=200, mimetype='application/json')


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def avg_genre_ratings_all_users():
    avg, _ = API.compute_avg_all_ratings_as_dict()
    return Response(str(avg), status=200, mimetype='application/json')


@app.route('/avg-genre-ratings/<int:user_id>', methods=['GET'])
def avg_genre_ratings_user(user_id):
    return Response(str(API.get_user_avg_ratings_as_dict(user_id=user_id)), status=200, mimetype='application/json')


@app.route('/profiles/<int:user_id>', methods=['GET'])
def get_user_profile(user_id):
    return Response(str(API.get_user_profile_as_dict(user_id=user_id)), status=200, mimetype='application/json')



if __name__ == '__main__':
    app.run()

