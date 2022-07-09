from cassandra.cluster import Cluster
import json
from flask import Flask, request
from flask_restful import Api, Resource

# Connect to Cassandra
# TODO: provide correct credentials
# TODO: replace insert by insert json
# TODO: add TTL
# TODO: set UTC time
# TODO: Add "No content code" 204
# https://docs.datastax.com/en/dse/6.0/cql/cql/cql_using/useInsertJSON.html


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
session.execute(table_ddl_start_session_events)

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
"""
# Start rest api server
app = Flask(__name__)
api = Api(app)


class PutSessions(Resource):
    def post(self):
        events = request.get_json()
        for event in events:
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
        return '', 201


class EndEventsByPlayer(Resource):
    def get(self, player_id):
        select_query_template_end_sessions = """
        SELECT JSON * FROM {table_name} WHERE player_id = '{player_id}' LIMIT 20;
        """
        select_query_end_sessions = select_query_template_end_sessions.format(
            table_name=table_name_end_session_events,
            player_id=player_id
        )
        query_response = session.execute(select_query_end_sessions)
        player_end_sessions_list = []
        for row in query_response:
            player_end_sessions_list.append(json.loads(row.json))
        return player_end_sessions_list, 200


api.add_resource(PutSessions, '/put_events')
api.add_resource(EndEventsByPlayer, '/end_players_events/<string:player_id>')

if __name__ == '__main__':
    app.run(debug=True)
