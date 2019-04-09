import redis
import json
import random
import time


r = redis.StrictRedis(host='localhost', port=6381, db=0)
while(1):
    a = random.randrange(0,100)
    b = random.randrange(0,100)
    c = random.randrange(0,100)

    r.rpush('zad3', json.dumps({'a': a, 'b': b, 'c': c}, sort_keys=True, indent=4))
    time.sleep(1)
