import redis
import time
import pandas as pd
import numpy as np
import client as c


while(1):
    print(c.get())
    time.sleep(0.1)