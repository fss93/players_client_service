# Create 10 countries
# Create 100 players. 10 players per country
# Generate 5 sessions per each player. Each session opens and closes. Total of 10 events per player

import hashlib
import random
from random import randrange
import datetime
import json

countries = ['AB', 'CD', 'EF', 'GH', 'IJ', 'KL', 'MN', 'OP', 'QR', 'ST']

players = []
for i in range(100):
    player_id = hashlib.md5(str(i).encode('ASCII')).hexdigest()
    players.append(player_id)

sessions = []
for i in range(100, 600):
    session_id = hashlib.md5(str(i).encode('ASCII')).hexdigest()
    sessions.append(session_id)


def random_start_date():
    start_date = datetime.datetime(2022, 1, 15, 18, 00)
    rand_date = start_date + datetime.timedelta(minutes=randrange(60 * 24 * 30 * 5))
    return rand_date.strftime("%Y-%m-%dT%H:%M:%S")


def random_end_date(start):
    start_date = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
    end_date = start_date + datetime.timedelta(minutes=randrange(60 * 15))
    return end_date.strftime("%Y-%m-%dT%H:%M:%S")


events = []
for i in range(500):
    start_dt = random_start_date()
    end_dt = random_end_date(start_dt)
    event_start = {'session_id': sessions[i],
                   'player_id': players[i//5],
                   'ts': start_dt,
                   'event': 'start',
                   'country': countries[i//50]
                   }
    event_end = {'session_id': sessions[i],
                 'player_id': players[i//5],
                 'ts': end_dt,
                 'event': 'end'
                 }
    events.append(event_start)
    events.append(event_end)

random.shuffle(events)

event_batches = []
cur_batch = []
for event in events:
    cur_batch.append(event)
    if len(cur_batch) == 10:
        event_batches.append(cur_batch)
        cur_batch = []

with open('test_case_simple_upload.txt', 'w', encoding='utf-8') as f:
    for batch in event_batches:
        f.write(json.dumps(batch) + '\n')
