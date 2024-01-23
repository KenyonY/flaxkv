import redis
from sparrow import MeasureTime

client = redis.Redis(host='localhost', port=6379, db=0)

# for i in range(10000):
#     client.set(f"key{i}", f"value{i}")

N = 10000
mt = MeasureTime()
for i in range(N):
    n = client.get(f"key{i}")

dt = mt.show_interval(f"GET {N} keys")
print(f"{N/dt} /s")