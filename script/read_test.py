import redis 
import lightkv_client



redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
client = lightkv_client.LightKVClient("127.0.1.1", "7100")

for i in range(10):
    file_name = '/home/lightman/repos/lightkv/script/concurrent/key' + str(i) + ".txt"
    f = open(file_name, 'r')
    lines = f.readlines()
    for line in lines:
        if line == ' ':
            continue 
        key = "key" + line[:-1]
        value, code = client.select(key)
        redis_value = redis_client.get(key)
        if value != redis_value:
            print("error")

    