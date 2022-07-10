"""
Generate test events and write them into file test_case_simple_upload.txt
Create 10 countries
Create 10 players. 1 players per country
Create 5 sessions per each player. Each session opens and closes. Total of 10 events per player
Events' timestamps are OLDER THAN 1 YEAR
"""

import hashlib
import random
from random import randrange
import datetime
import json

countries = ['AB', 'CD', 'EF', 'GH', 'IJ', 'KL', 'MN', 'OP', 'QR', 'ST']

# Create 10 players (1 per country)
players = []
for i in range(10):
    player_id = hashlib.md5(str(i).encode('ASCII')).hexdigest()
    players.append(player_id)

# Create 50 sessions (5 per player)
sessions = []
for i in range(10, 60):
    session_id = hashlib.md5(str(i).encode('ASCII')).hexdigest()
    sessions.append(session_id)


# Start date older than 1 year
def outdated_start_date():
    outdated_date = datetime.datetime.now() - datetime.timedelta(days=366)
    return outdated_date.strftime("%Y-%m-%dT%H:%M:%S")


# End date older than year
def random_end_date(start):
    start_date = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
    end_date = start_date + datetime.timedelta(minutes=randrange(60))
    return end_date.strftime("%Y-%m-%dT%H:%M:%S")


# Generate start and end events
events = []
for i in range(50):
    start_dt = outdated_start_date()
    end_dt = random_end_date(start_dt)
    event_start = {'session_id': sessions[i],
                   'player_id': players[i//5],
                   'ts': start_dt,
                   'event': 'start',
                   'country': countries[i//5]
                   }
    event_end = {'session_id': sessions[i],
                 'player_id': players[i//5],
                 'ts': end_dt,
                 'event': 'end'
                 }
    events.append(event_start)
    events.append(event_end)

# Shuffle events to check if Cassandra sorting and grouping by country
random.shuffle(events)

# Group events into batches
event_batches = []
cur_batch = []
for event in events:
    cur_batch.append(event)
    if len(cur_batch) == 10:
        event_batches.append(cur_batch)
        cur_batch = []

with open('test_case_outdated_events.txt', 'w', encoding='utf-8') as f:
    for batch in event_batches:
        f.write(json.dumps(batch) + '\n')
