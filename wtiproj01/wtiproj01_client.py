import redis
zmienna ="wartosc"
print(zmienna)
r = redis.RedisStrict(host='localhost', port=6381, db=0)
r.set('foo', zmienna)
print(r.get('name'))