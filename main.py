from cassandra.cluster import Cluster
import json

# Connect to Cassandra
# TODO: provide correct credentials
cluster = Cluster()
session = cluster.connect()

# Initialize keyspace
keyspace_name_player_events = 'player_events'
keyspace_ddl_player_events = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace_name_player_events}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    AND durable_writes = true;
"""
session.execute(keyspace_ddl_player_events)
session.set_keyspace(keyspace_name_player_events)

# Initialize start_session_events table
table_name_start_session_events = 'start_session_events'
table_ddl_start_session_events = f"""
CREATE TABLE IF NOT EXISTS {table_name_start_session_events} (
    country text,
    ts timestamp,
    session_id text,
    event text,
    player_id text,
    PRIMARY KEY (country, ts, session_id)
) WITH CLUSTERING ORDER BY (ts DESC, session_id ASC);
"""

# Initialize end_session_events table
table_name_end_session_events = 'end_session_events'
table_ddl_end_session_events = f"""
CREATE TABLE IF NOT EXISTS {table_name_end_session_events} (
    player_id text,
    ts timestamp,
    session_id text,
    event text,
    PRIMARY KEY (player_id, ts, session_id))
    WITH CLUSTERING ORDER BY (ts DESC, session_id ASC);
"""
session.execute(table_ddl_end_session_events)

# Insert query templates
insert_query_template_start_session = """
    INSERT INTO {table_name} (country, player_id, ts, session_id, event)
    VALUES ('{country}', '{player_id}', '{ts}', '{session_id}', '{event_type}')
"""

insert_query_template_end_session = """
    INSERT INTO {table_name} (player_id, ts, session_id, event)
    VALUES ('{player_id}', '{ts}', '{session_id}', '{event_type}')
"""

test_case_simple_upload_f = open('test_case_simple_upload.txt', encoding='utf-8')
for batch_str in test_case_simple_upload_f:
    batch = json.loads(batch_str)
    for event in batch:
        if event.get('event') == 'start':
            insert_query_start_session = insert_query_template_start_session.format(
                table_name=table_name_start_session_events,
                country=event.get('country'),
                player_id=event.get('player_id'),
                ts=event.get('ts'),
                session_id=event.get('session_id'),
                event_type=event.get('event')
            )
            session.execute(insert_query_start_session)
        elif event.get('event') == 'end':
            insert_query_end_session = insert_query_template_end_session.format(
                table_name=table_name_end_session_events,
                player_id=event.get('player_id'),
                ts=event.get('ts'),
                session_id=event.get('session_id'),
                event_type=event.get('event')
            )
            session.execute(insert_query_end_session)

test_case_simple_upload_f.close()
