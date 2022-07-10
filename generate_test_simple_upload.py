"""
Generate test events and write them into file test_case_simple_upload.txt
Create 10 countries
Create 100 players. 10 players per country
Create 5 sessions per each player. Each session opens and closes. Total of 10 events per player
Events' timestamps are not older than 1 month
"""

import hashlib
import random
from random import randrange
import datetime
import json


class RecentTestCase:
    countries = ['AB', 'CD', 'EF', 'GH', 'IJ', 'KL', 'MN', 'OP', 'QR', 'ST']

    def random_start_date(self):
        """Generate date not older than 1 month"""
        rand_date = datetime.datetime.now() - datetime.timedelta(minutes=randrange(60 * 24 * 30))
        return rand_date.strftime("%Y-%m-%dT%H:%M:%S")

    def random_end_date(self, start):
        """Generate date within 15 hours from start_date"""
        start_date = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
        end_date = start_date + datetime.timedelta(minutes=randrange(60 * 15))
        return end_date.strftime("%Y-%m-%dT%H:%M:%S")

    def generate_sample(self, players_per_country, sessions_per_player, file_name):
        """
        Generate test batches
        """
        # Total number of players to generate
        number_of_players = players_per_country * len(self.countries)
        # Total number of sessions to generate
        number_of_sessions = number_of_players * sessions_per_player
        sessions_per_country = sessions_per_player * players_per_country

        players = []
        for i in range(number_of_players):
            player_id = hashlib.md5(str(i).encode('ASCII')).hexdigest()
            players.append(player_id)

        sessions = []
        for i in range(number_of_players, number_of_players + number_of_sessions):
            session_id = hashlib.md5(str(i).encode('ASCII')).hexdigest()
            sessions.append(session_id)

        # Generate start and end events
        events = []
        for i in range(number_of_sessions):
            start_dt = self.random_start_date()
            end_dt = self.random_end_date(start_dt)
            event_start = {'session_id': sessions[i],
                           'player_id': players[i//sessions_per_player],
                           'ts': start_dt,
                           'event': 'start',
                           'country': self.countries[i//sessions_per_country]
                           }
            event_end = {'session_id': sessions[i],
                         'player_id': players[i//sessions_per_player],
                         'ts': end_dt,
                         'event': 'end'
                         }
            events.append(event_start)
            events.append(event_end)

        # Shuffle events to check if Cassandra sorting and grouping by country
        random.shuffle(events)

        # Group events into batches
        event_batches = []
        cur_batch = []
        for event in events:
            cur_batch.append(event)
            if len(cur_batch) == 10:
                event_batches.append(cur_batch)
                cur_batch = []

        with open(file_name, 'w', encoding='utf-8') as f:
            for batch in event_batches:
                f.write(json.dumps(batch) + '\n')


class OutdatedTestCase(RecentTestCase):
    def random_start_date(self):
        """Generate date older than 1 year"""
        outdated_date = datetime.datetime.now() - datetime.timedelta(days=366)
        return outdated_date.strftime("%Y-%m-%dT%H:%M:%S")


if __name__ == '__main__':
    recent_case = RecentTestCase()
    recent_case.generate_sample(
        players_per_country=10,
        sessions_per_player=5,
        file_name='test_case_simple_upload.txt'
    )
    outdated_case = OutdatedTestCase()
    outdated_case.generate_sample(
        players_per_country=1,
        sessions_per_player=5,
        file_name='test_case_outdated_events.txt'
    )
