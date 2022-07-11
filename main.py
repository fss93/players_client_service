import datetime
import json
from cassandra.cluster import Cluster
from flask import Flask, request
from flask_restful import Api, Resource

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


# Init rest api server
app = Flask(__name__)
api = Api(app)


class PutSessions(Resource):
    """Insert batch of events into database"""
    insert_query_template = """
                INSERT INTO {table_name}
                JSON '{event_json}'
                USING TTL {ttl};
            """

    def post(self):
        # Get batch as json
        events = request.get_json()
        for event in events:
            # Calculate time to live in seconds
            event_datetime = datetime.datetime.strptime(event.get('ts'), '%Y-%m-%dT%H:%M:%S')
            expiration_datetime = event_datetime + datetime.timedelta(days=365)
            ttl = (expiration_datetime - datetime.datetime.now()).total_seconds()
            ttl = int(ttl)
            # Records older than 365 days are ignored
            if ttl <= 0:
                continue
            if event.get('event') == 'start':
                # Insert start event
                insert_query_start_session = self.insert_query_template.format(
                    table_name=table_name_start_session_events,
                    event_json=json.dumps(event),
                    ttl=ttl
                )
                session.execute(insert_query_start_session)
            elif event.get('event') == 'end':
                # Insert end event
                insert_query_end_session = self.insert_query_template.format(
                    table_name=table_name_end_session_events,
                    event_json=json.dumps(event),
                    ttl=ttl
                )
                session.execute(insert_query_end_session)
        return '', 201


class EndEventsByPlayer(Resource):
    select_query_template_end_sessions = """
            SELECT JSON * FROM {table_name} WHERE player_id = '{player_id}' LIMIT 20;
            """

    def get(self, player_id):
        # Fetch the last 20 records for a given player
        select_query_end_sessions = self.select_query_template_end_sessions.format(
            table_name=table_name_end_session_events,
            player_id=player_id
        )
        query_response = session.execute(select_query_end_sessions)
        player_end_sessions_list = []
        for row in query_response:
            player_end_sessions_list.append(json.loads(row.json))
        return player_end_sessions_list, 200


class RecentStartEvents(Resource):
    select_query_template_start_sessions = """
                    SELECT JSON *
                    FROM {table_name}
                    WHERE ts >= '{relevance_datetime:%Y-%m-%d %H:%M:%S}'
                    ALLOW FILTERING;
                    """

    def get(self, hrs):
        # Fetch sessions for the last hrs hours grouped by country
        relevance_datetime = datetime.datetime.now() - datetime.timedelta(hours=float(hrs))
        select_query_start_sessions = self.select_query_template_start_sessions.format(
            table_name=table_name_start_session_events,
            relevance_datetime=relevance_datetime
        )
        query_response = session.execute(select_query_start_sessions)
        player_start_sessions_list = []
        for row in query_response:
            player_start_sessions_list.append(json.loads(row.json))
        return player_start_sessions_list, 200


# Add rest api resources
api.add_resource(PutSessions, '/put_events')
api.add_resource(EndEventsByPlayer, '/end_players_events/<string:player_id>')
api.add_resource(RecentStartEvents, '/start_players_events/<string:hrs>')

if __name__ == '__main__':
    app.run()
