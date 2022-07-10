# Player session rest-api service

The Player Session Service is designed to store and retrieve information about
player's sessions

## Requirements
The service uses Cassandra 4.0.4 database. All necessary python packages are
listed in `requirements.txt` file

```shell
pip install -r requirements.txt
```

### Initial Configuration

By default, service connects to a Cassandra instance on your local machine
(127.0.0.1). You can also specify a list of IP addresses for nodes in `main.py`

### Running

To run service simply use

```shell
python main.py
```


## Features

#### Post batch of player's sessions
Use `POST` method to send a batch of 10 sessions on http://127.0.0.1:5000/put_events
as a json object

#### Fetch last 20 completed sessions for a given player
Use `GET` method to fetch last 20 completed sessions for a given `player_id`

http://127.0.0.1:5000/end_players_events/player_id

#### Fetch start sessions for the last `x` hours for all countries
Use `GET` method to fetch start sessions for the last `x` hours. `x` might be
either integer or float. Sessions will be grouped by country.

http://127.0.0.1:5000/start_players_events/0.5
