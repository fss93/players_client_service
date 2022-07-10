import json
import requests
import datetime
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('player_events')

base = 'http://127.0.0.1:5000/'

# Test 1
# Put 500 start events and 500 end events
post_latency = []  # container for latency times for PUT operations
players = set()  # all players for future tests
with open('test_case_simple_upload.txt', encoding='utf-8') as f:
    for batch in f:
        batch = json.loads(batch)
        start_time = datetime.datetime.now()
        requests.post(base + 'put_events', json=batch)
        end_time = datetime.datetime.now()
        post_latency.append((end_time-start_time).total_seconds() * 1000)
        for event in batch:
            players.add(event.get('player_id'))

# Check write
start_query = "SELECT count(*) FROM start_session_events;"
start_resp = session.execute(start_query)
assert start_resp.one().count == 500

end_query = "SELECT count(*) FROM end_session_events;"
end_resp = session.execute(end_query)
assert end_resp.one().count == 500

# Test 2
# Check write latency
# 90% of writes must be faster than 500 ms
slow_writes = 0
total_writes = len(post_latency)
for lat in post_latency:
    if lat >= 500:
        slow_writes += 1

assert slow_writes / total_writes < 0.1

# Test 3
# Post events older than 1 year
with open('test_case_outdated_events.txt', encoding='utf-8') as f:
    for batch in f:
        batch = json.loads(batch)
        requests.post(base + 'put_events', json=batch)
        end_time = datetime.datetime.now()

# They shouldn't be posted. Remains 500 lines in each table
start_resp = session.execute(start_query)
assert start_resp.one().count == 500

end_resp = session.execute(end_query)
assert end_resp.one().count == 500

# Test 4
# Check TTL
# now() + TTL = ts + 1 year
ttl_query = "SELECT ts, TTL(event) FROM start_session_events LIMIT 10;"
ttl_resp = session.execute(ttl_query)
for row in ttl_resp:
    # Calculate error in seconds
    error = (datetime.datetime.utcnow()
             + datetime.timedelta(seconds=row.ttl_event)
             - row.ts
             - datetime.timedelta(days=365)
             ).total_seconds()
    assert error < 10

# Test 5
# Check player_id
# For each player_id were generated 5 end sessions
end_sessions = requests.get(base + 'end_players_events/' + '32bb90e8976aab5298d5da10fe66f21d')
assert len(end_sessions.json()) == 5

for event in end_sessions.json():
    assert event.get('player_id') == '32bb90e8976aab5298d5da10fe66f21d'

# Test 6
# Check sessions for last X hours
x = 100
start_sessions = requests.get(base + f'start_players_events/{x}')
for event in start_sessions.json():
    # Check that fetched timestamp is not older than X hours
    event_datetime = datetime.datetime.strptime(event.get('ts'), '%Y-%m-%d %H:%M:%S.000Z')
    assert event_datetime >= datetime.datetime.utcnow() - datetime.timedelta(hours=x)

# Test 7
# Check get_end_sessions latency
# 90% of reads must be faster than 500 ms
slow_reads = 0
for player_id in players:
    start_time = datetime.datetime.now()
    requests.get(base + f'end_players_events/{player_id}')
    end_time = datetime.datetime.now()
    latency = (end_time-start_time).total_seconds() * 1000
    if latency > 500:
        slow_reads += 1

assert slow_reads / len(players) < 0.1


# Test 8
# Check get_start_sessions latency
# 90% of reads must be faster than 500 ms
slow_reads = 0
for hrs in range(100):
    start_time = datetime.datetime.now()
    resp = requests.get(base + f'start_players_events/{hrs}')
    end_time = datetime.datetime.now()
    latency = (end_time-start_time).total_seconds() * 1000
    if latency > 500:
        slow_reads += 1

assert slow_reads / 100 < 0.1
