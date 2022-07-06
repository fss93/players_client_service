import requests

base = 'http://127.0.0.1:5000/'
response = requests.get(base + 'helloworld')
print(response.json())

response_2 = requests.post(base + 'helloworld')
print(response_2.json())
