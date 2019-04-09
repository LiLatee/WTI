import redis
import json
import time


r = redis.StrictRedis(host='localhost', port=6381, db=0)
while(1):
    # wartosc = r.lrange('kolejka', 0, 0)[0]
    wartosc = r.lpop('zad3')
    print(json.loads(wartosc))
    time.sleep(1)



