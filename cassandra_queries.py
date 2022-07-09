from cassandra.cluster import Cluster
import json

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('player_events')
query = """
SELECT JSON * FROM end_session_events WHERE player_id = '32bb90e8976aab5298d5da10fe66f21d' LIMIT 20;
"""

t = session.execute(query)
res = []
for row in t:
    res.append(json.loads(row.json))


print(json.dumps(res))
