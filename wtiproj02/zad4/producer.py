import redis
import time
import pandas as pd
import numpy as np


r = redis.StrictRedis(host="localhost", port=6381, db=0)
data = []
with open('user_ratedmovies.dat', 'r') as file:
    d = file.readlines()
    for i in d:
       k = i.split()
       data.append(k)


data = np.array(data, dtype='O')
df = pd.DataFrame(data[1::], columns=data[0])

for x in range(0, df.last_valid_index()+1):
    row = df.loc[x].to_json(orient='index')
    r.rpush('zad4', row)
    time.sleep(1)



