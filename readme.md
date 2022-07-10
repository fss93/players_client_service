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
Use `POST` method to send a batch of 10 events on http://127.0.0.1:5000/put_events
as a json object

#### Fetch last 20 completed sessions for a given player
Use `GET` method to fetch last 20 completed sessions for a given `player_id`

http://127.0.0.1:5000/end_players_events/player_id

#### Fetch start sessions for the last `x` hours for all countries
Use `GET` method to fetch start sessions for the last `x` hours. `x` might be
either integer or float. Sessions will be grouped by country.

http://127.0.0.1:5000/start_players_events/0.5

## Tests
#### Manual tests
To test the service manually, start it using command
```shell
python main.py
```
Run file `client.py` in IDE to test interaction with rest api server. Firstly,
it creates file `test_case_simple_upload.txt` which contains batches of events
for testing. Then it uploads test cases to the server. Use
different `player_id` and `hours` to visually check output.
#### Auto tests
To test the service automatically, start the server using command
```shell
python main.py
```
Run file `auto_test.py`. Firstly, it creates two files with test batches.
`test_case_simple_upload.txt` contains batches of recent events (not older than
1 month).
`test_case_outdated_events.txt` contains events older than 1 year.
* Test 1. Upload batches from `test_case_simple_upload.txt` to Cassandra
using rest api. Check number of rows in database
* Test 2. Check latency for put method. It should be less than 500 ms in 90%
cases
* Test 3. Try to post events older than 1 year. Check that they are not posted.
* Test 4. Check TTL. Events older than 1 year should be removed from Cassandra
* Test 5. Check `end_players_events` method for a given `player_id`
* Test 6. Check `start_players_events` method. 
* Test 7. Check latency for `end_players_events` method. It should be less
than 500 ms in 90% cases
* Test 8. Check latency for `start_players_events` method. It should be less
than 500 ms in 90% cases
