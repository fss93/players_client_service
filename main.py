from cassandra.cluster import Cluster

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

