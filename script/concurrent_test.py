import redis 
import lightkv_client
import random
from concurrent.futures import ThreadPoolExecutor

def run(base):
    start = base * 1000
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    client = lightkv_client.LightKVClient("127.0.1.1", "7100")
    indexs = []
    x = 0
    y = 0
    for i in range(start, (base+1) * 1000):
        if random.random() > 0.3:   # insert
            key = "key"+str(i)
            value = "value"+str(i)
            client.insert(key, value)
            redis_client.set(key, value)
            indexs.append(i)
        else:   # delete
            if len(indexs) > 0:
                delete_key = "key"+str(indexs[0])
                del indexs[0]
                client.delete(delete_key)
                redis_client.delete(delete_key)
    file_name = '/home/lightman/repos/lightkv/script/concurrent/key' + str(base) + ".txt"
    f = open(file_name, 'w')
    for i in range(len(indexs)):
        f.write(str(indexs[i]) + "\n")


if __name__ == '__main__':
    executor = ThreadPoolExecutor(max_workers=10)
    futures = []
    for i in range(10):
        future = executor.submit(run, i)
        futures.append(future)
    for i in range(10):
        futures[i].result()

