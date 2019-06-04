import requests

def show_details_of_request(r, method):
    print('Request')
    print('\tUrl: ' + str(r.url))
    print('\tMethod: ' + method)
    print('\tBody: ' + str(r.request.body))

    print('Response')
    print('\tCode: ' + str(r.status_code))
    print('\tContent: ' + str(r.text))
    print('\tHeaders: ' + str(r.headers))

    # print('request.status_code:' + str(r.status_code))
    # print('request.headers: ' + str(r.headers))
    # print('request.text: ' + str(r.text))
    # # data = json.loads(r.text)
    # # for key, value in data.items():
    # #     print(value)
    # #     time.sleep(0.1)
    # print('request.request.body: ' + str(r.request.body))
    # print('request.request.header: ' + str(r.request.headers))
    print('\n\n')


def get_preselected_movies_for_user(user_id):
    r = requests.get('http://localhost:5000/user/preselection/' + str(user_id))
    show_details_of_request(r, "GET")

def get_preselected_users_for_movie(movie_id):
    r = requests.get('http://localhost:5000/movie/preselection/' + str(movie_id))
    show_details_of_request(r, "GET")

def get_user(user_id):
    r = requests.get('http://localhost:5000/user/document/' + str(user_id))
    show_details_of_request(r, "GET")

def get_movie(movie_id):
    r = requests.get('http://localhost:5000/movie/document/' + str(movie_id))
    show_details_of_request(r, "GET")

def add_user(user_id, movies_list):
    r = requests.post('http://localhost:5000/user/document/' + str(user_id), data=movies_list)
    show_details_of_request(r, "POST")

def add_movie(movie_id, users_list):
    r = requests.post('http://localhost:5000/movie/document/' + str(movie_id), data=users_list)
    show_details_of_request(r, "POST")

def update_user_movies(user_id, movies_list):
    r = requests.put('http://localhost:5000/user/document/' + str(user_id), data=movies_list)
    show_details_of_request(r, "PUT")

def update_movie_users(movie_id, users_list):
    r = requests.put('http://localhost:5000/movie/document/' + str(movie_id), data=users_list)
    show_details_of_request(r, "PUT")

def delete_user(user_id):
    r = requests.delete('http://localhost:5000/user/document/' + str(user_id))
    show_details_of_request(r, "DELETE")

def delete_movie(movie_id):
    r = requests.delete('http://localhost:5000/movie/document/' + str(movie_id))
    show_details_of_request(r, "DELETE")

def exists_user(user_id):
    r = requests.get('http://localhost:5000/user/document/exists/' + str(user_id))
    show_details_of_request(r, "GET")

def exists_movie(movie_id):
    r = requests.get('http://localhost:5000/movie/document/exists/' + str(movie_id))
    show_details_of_request(r, "GET")


import time

if __name__ == '__main__':
    # get_preselection_for_user(75)
    # get_preselection_for_movie(3)

    # get_user(-1)
    # get_movie(3)

    # add_user(111, '{"ratings": [1, 2,3]}')
    # add_movie(-10, '{"whoRated": [1, 2]}')
    # get_user(-1)
    # get_movie(-10)

    # get_user(75)
    # time.sleep(1)
    # delete_user(75)
    # get_user(75)

    # exists_user(75)
    # exists_user(78)
    # exists_movie(4270)

    print("lista preselekcyjna movies dla usera 75")
    get_preselected_movies_for_user(75)
    print("lista preselekcyjna userw dla movie 3")
    get_preselected_users_for_movie(3)

    ### 1 ###
    print("dodaj movie -1")
    add_movie(-1, '{"whoRated": [1, 2]}')
    time.sleep(1)
    print("dodaj usera -1 ktory lubi movie -1")
    add_user(-1, '{"ratings": [-1]}')
    time.sleep(1)
    print("user -1")
    get_user(-1)
    print("movie -1")
    get_movie(-1)
    print("dodaj userowi -1 movies 1 i 2")
    update_user_movies(-1, '{"ratings": [1, 2]}')
    time.sleep(1)
    print("user -1")
    get_user(-1)
    print("movie 1")
    get_movie(1)
    print("usun movie -1")
    delete_movie(-1)
    time.sleep(1)
    print("user -1")
    get_user(-1)
    print("usun user -1")
    delete_user(-1)
    print("sprawdz czy user -1 istnieje ")
    exists_user(-1)
