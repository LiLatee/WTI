import redis
import time
import pandas as pd
import numpy as np
import client as c


while(True):
    # data = []
    # with open('user_ratedmovies.dat', 'r') as file:
    #     d = file.readlines()
    #     for i in d:
    #         k = i.split()
    #         data.append(k)
    #
    # data = np.array(data, dtype='O')
    # df = pd.DataFrame(data[1::], columns=data[0])

    #df albo df2 uywa

    df2 = pd.read_csv('user_ratedmovies.dat', sep='\t', nrows=100)
    for x in range(0, df2.last_valid_index()+1):
        row = df2.loc[x].to_json(orient='index')
        c.add(row);
        time.sleep(1)



