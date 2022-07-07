from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('player_events')
query = """
SELECT * FROM end_session_events WHERE player_id = '32bb90e8976aab5298d5da10fe66f21d' LIMIT 20;
"""

t = session.execute(query)
for row in t:
    print(row.player_id, row.ts)
