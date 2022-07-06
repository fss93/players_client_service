from cassandra.cluster import Cluster
import json

# Connect to Cassandra
# TODO: provide correct credentials
cluster = Cluster()
session = cluster.connect()

# Initialize keyspace
keyspace_player_events_name = 'player_events'
keyspace_player_events_ddl = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace_player_events_name}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    AND durable_writes = true;
"""
session.execute(keyspace_player_events_ddl)
session.set_keyspace(keyspace_player_events_name)

# Initialize end_session_events table
table_end_session_events_name = 'end_session_events'
table_end_session_events_ddl = f"""
CREATE TABLE IF NOT EXISTS {table_end_session_events_name} (
    player_id text,
    ts timestamp,
    session_id text,
    event text,
    PRIMARY KEY (player_id, ts, session_id))
    WITH CLUSTERING ORDER BY (ts DESC, session_id ASC);
"""
session.execute(table_end_session_events_ddl)

"""
with open('/home/sergey/Desktop/unity/assignment_data.jsonl') as input_f:
    for _ in range(100):
        str_data = input_f.readline().strip()
        js_data = json.loads(str_data)
        if js_data.get('event') == 'end':
            q = f"INSERT INTO {table_end_session_events_name} (player_id, ts, session_id, event) VALUES ('{js_data.get('player_id')}', '{js_data.get('ts')}', '{js_data.get('session_id')}', 'end')"
            session.execute(q)
"""

with open('/home/sergey/Desktop/unity/assignment_data.jsonl') as input_f:
    for _ in range(1000):
        str_data = input_f.readline().strip()
        js_data = json.loads(str_data)
        if js_data.get('event') == 'start':
            q = f"INSERT INTO start_session_events (country, player_id, ts, session_id, event) VALUES ('{js_data.get('country')}', '{js_data.get('player_id')}', '{js_data.get('ts')}', '{js_data.get('session_id')}', 'start')"
            session.execute(q)
