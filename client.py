import requests

base = 'http://127.0.0.1:5000/'
response = requests.get(base + 'helloworld')
print(response.json())

response_2 = requests.post(base + 'helloworld', json=[{'a': 1}, {'b': 2}])
print(response_2.json())
