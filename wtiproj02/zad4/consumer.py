import redis
import json
import time


r = redis.StrictRedis(host='localhost', port=6381, db=0)

# for x in range(3):
#     # wartosc = r.lrange('kolejka', 0, 0)[0]
#     #print(r.lrange('zad4', 0, -1))
#     wartosc = r.lpop('zad4')
#     print(json.loads(wartosc))
#     time.sleep(1)


r = redis.StrictRedis(host='localhost', port=6381, db=0)
while(1):
    if(r.exists('zad4')):
        # wartosc = r.lrange('kolejka', 0, 0)[0]
        #print(r.lrange('zad4', 0, -1))
        wartosc = r.lpop('zad4')
        if (wartosc != None):
            print(json.loads(wartosc))
        time.sleep(1)
