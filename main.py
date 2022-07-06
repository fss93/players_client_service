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

"""
with open('/home/sergey/Desktop/unity/assignment_data.jsonl') as input_f:
    for _ in range(1000):
        str_data = input_f.readline().strip()
        js_data = json.loads(str_data)
        if js_data.get('event') == 'start':
            q = f"INSERT INTO start_session_events (country, player_id, ts, session_id, event) VALUES ('{js_data.get('country')}', '{js_data.get('player_id')}', '{js_data.get('ts')}', '{js_data.get('session_id')}', 'start')"
            session.execute(q)
"""

insert_query_template_start_session = """
    INSERT INTO start_session_events (country, player_id, ts, session_id, event)
    VALUES ('{country}', '{player_id}', '{ts}', '{session_id}', '{event_type}')
"""

insert_query_template_end_session = """
    INSERT INTO end_session_events (player_id, ts, session_id, event)
    VALUES ('{player_id}', '{ts}', '{session_id}', '{event_type}')
"""

test_case_simple_upload_f = open('test_case_simple_upload.txt', encoding='utf-8')
for batch_str in test_case_simple_upload_f:
    batch = json.loads(batch_str)
    for event in batch:
        if event.get('event') == 'start':
            insert_query_start_session = insert_query_template_start_session.format(
                country=event.get('country'),
                player_id=event.get('player_id'),
                ts=event.get('ts'),
                session_id=event.get('session_id'),
                event_type=event.get('event')
            )
            session.execute(insert_query_start_session)
        elif event.get('event') == 'end':
            insert_query_end_session = insert_query_template_end_session.format(
                player_id=event.get('player_id'),
                ts=event.get('ts'),
                session_id=event.get('session_id'),
                event_type=event.get('event')
            )
            session.execute(insert_query_end_session)

test_case_simple_upload_f.close()
