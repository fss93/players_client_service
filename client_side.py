import json
import requests

base = 'http://127.0.0.1:5000/'
# with open('test_case_simple_upload.txt', encoding='utf-8') as f:
#     for batch in f:
#         batch = json.loads(batch)
#         requests.post(base + 'put_events', json=batch)

r = requests.get(base + 'end_players_events/' + '32bb90e8976aab5298d5da10fe66f21d')
print(r.json())
