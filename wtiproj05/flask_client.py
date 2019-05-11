import requests
import sys
import os
import json
import time


def show_details_of_request(r):
    print('request.url:' + str(r.url))
    print('request.status_code:' + str(r.status_code))
    print('request.headers: ' + str(r.headers))
    print('request.text: ' + str(r.text))
    # data = json.loads(r.text)
    # for key, value in data.items():
    #     print(value)
    #     time.sleep(0.1)
    print('request.request.body: ' + str(r.request.body))
    print('request.request.header: ' + str(r.request.headers))
    print('\n\n')

def get_all_ratings():
    r = requests.get('http://localhost:5000/ratings')
    show_details_of_request(r)

def avg_genre_ratings_all_users():
    r = requests.get('http://localhost:5000/avg-genre-ratings/all-users')
    show_details_of_request(r)


def avg_genre_ratings_user(user_id):
    r = requests.get('http://localhost:5000/avg-genre-ratings/' + str(user_id))
    show_details_of_request(r)


def add_rating(data):
    r = requests.post('http://localhost:5000/ratings', data= data)
    show_details_of_request(r)

def delete_all_ratings():
    r = requests.delete('http://localhost:5000/ratings')
    show_details_of_request(r)

def get_user_profile(user_id):
    r = requests.get('http://localhost:5000/profiles/' + str(user_id))
    show_details_of_request(r)

if __name__ == '__main__':
#     menu ='''======MENU======
# 1. GET ratings
# 2. GET avg-genre-ratings/all-users'
# 3. GET avg-genre-ratings/user_id
# 4. POST rating
# 5. DELETE rating
# 6. GET user profile
# 7. Wyczysc
# 8. Wyjdz
# '''
    print("wszystkie oceny z plikow")
    print(get_all_ratings())
    print("srednia wszystkich z plikow")
    print(avg_genre_ratings_all_users())
    print("srednia usera 75 z plikow")
    print(avg_genre_ratings_user(75))
    print("profil usera 75 z plikow")
    print(get_user_profile(75))
    print("czyszczenie bazy")
    print(delete_all_ratings())
    print("kilka postow")
    print(add_rating('{"userID": 1,"movieID": 3,"rating": 1,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}'))
    print(add_rating('{"userID": 1,"movieID": 2,"rating": 4,"genre_Adventure": 1,"genre_Comedy": null,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}'))
    print(add_rating('{"userID": 1,"movieID": 3,"rating": 5,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}'))
    print(add_rating('{"userID": 2,"movieID": 3,"rating": 3,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}'))
    print(add_rating('{"userID": 1,"movieID": 3,"rating": 1,"genre_Adventure": null,"genre_Comedy": 1,"genre_Drama": null,"genre_Fantasy": null,"genre_Mystery": null,"genre_Romance": 1,"genre_Sci-Fi": null,"genre_Thriller": null,"genre_War": null}'))
    print("srednia wszystkich z nowych danych")
    print(avg_genre_ratings_all_users())
    print("srednia usera 1 z nowych danych")
    print(avg_genre_ratings_user(1))
    print("profil usera 1 z nowych danych")
    print(get_user_profile(1))


    # while(True):
    #     print(menu)
    #     action = input("Wybierz akcje:")
    #
    #     if action == '1':
    #         get_all_ratings()
    #     elif action == '2':
    #         avg_genre_ratings_all_users()
    #     elif action == '3':
    #         user_id = input('Podaj ID usera: ')
    #         avg_genre_ratings_user(user_id)
    #     elif action == '4':
    #         data = input('Podaj dane w formacie JSON: ')
    #         print(data)
    #         add_rating(data)
    #     elif action == '5':
    #         delete_all_ratings()
    #     elif action == '6':
    #         user_id = input('Podaj ID usera: ')
    #         get_user_profile(user_id)
    #     elif action == '7':
    #         os.system('clear')
    #     elif action == '8':
    #         print('Paaaa')
    #         sys.exit()

