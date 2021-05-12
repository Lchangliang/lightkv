import lightkv_client
client = lightkv_client.LightKVClient("127.0.1.1", "7100")
key = "hello"
value, message = client.select(key)
if message == "":
    print(value)
else:
    print(message)