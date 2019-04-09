import redis
import time
import pandas as pd
import numpy as np



def get():
    r = redis.StrictRedis(host='localhost', port=6381, db=0)
    if (r.exists('zad4') == None):
        return None
    result = r.lrange('zad4',0,-1)
    clear(len(result))
    return result;


def add(row):
    r = redis.StrictRedis(host="localhost", port=6381, db=0)
    r.rpush('zad4', row)


def clear(start):
    r = redis.StrictRedis(host='localhost', port=6381, db=0)
    r.ltrim('zad4', start, -1)
    #r.ltrim('zad4',r.llen('zad4')+1,r.llen('zad4')+2)
    #r.delete('zad4')

