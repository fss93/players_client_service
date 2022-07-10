from cassandra.cluster import Cluster
import json
import datetime

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('player_events')

hrs = 700.5
dttm = datetime.datetime.now() - datetime.timedelta(hours=hrs)
query = f"""
SELECT JSON * FROM start_session_events WHERE ts >= '{dttm:%Y-%m-%d %H:%M:%S}' ALLOW FILTERING;
"""

t = session.execute(query)

res = []
for row in t:
    res.append(json.loads(row.json))

print(dttm)
for x in res:
    print(x)

event_date = datetime.datetime(2022, 5, 26, 19, 48, 14)
expiration_date = event_date - datetime.timedelta(days=365)
ttl_seconds = (event_date - expiration_date).total_seconds()
negative_diff = (expiration_date - event_date).total_seconds()
print(event_date, expiration_date, ttl_seconds, negative_diff)
