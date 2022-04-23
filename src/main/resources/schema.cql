CREATE KEYSPACE IF NOT EXISTS streaming
    WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS streaming.multiplayer_score_event (
    id uuid,
    player_id int,
    player_name text,
    player_score int,
    PRIMARY KEY (id)
);