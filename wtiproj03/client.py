import requests
import sys
import os
import json
import time


def showDetails(r):
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

def getRating():
    r = requests.get('http://localhost:5000/ratings')
    showDetails(r)

def avg_genre_ratings_all_users():
    r = requests.get('http://localhost:5000/avg-genre-ratings/all-users')
    showDetails(r)


def avg_genre_ratings_user(user_id):
    r = requests.get('http://localhost:5000/avg-genre-ratings/' + user_id)
    showDetails(r)


def addRating(data):
    r = requests.post('http://localhost:5000/rating', data= data)
    showDetails(r)

def deleteRating():
    r = requests.delete('http://localhost:5000/rating')
    showDetails(r)



if __name__ == '__main__':
    menu ='''======MENU======
1. GET ratings
2. GET avg-genre-ratings/all-users'
3. GET avg-genre-ratings/userID
4. POST rating
5. DELETE rating
5. Wyczysc
6. Wyjdz
'''

    while(True):
        print(menu)
        action = input("Wybierz akcje:")

        if action == '1':
            getRating()
        elif action == '2':
            avg_genre_ratings_all_users()
        elif action == '3':
            userID = input('Podaj ID usera: ')
            avg_genre_ratings_user(userID)
        elif action == '4':
            data = input('Podaj dane w formacie JSON: ')
            print(data)
            addRating(data)
        elif action == '5':
            deleteRating()
        elif action == '6':
            os.system('clear')
        elif action == '7':
            print('Paaaa')
            sys.exit()

