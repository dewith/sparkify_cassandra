#%% Tables creation
session_table_create = '''
    CREATE TABLE IF NOT EXISTS session_library (
        session_id bigint, 
        item_in_session int, 
        artist_name text, 
        song_title text, 
        song_length float,
        PRIMARY KEY (session_id, item_in_session)
    )  
'''

user_table_create = '''
    CREATE TABLE IF NOT EXISTS user_library (
        user_id bigint,
        session_id bigint, 
        item_in_session int,
        artist_name text,
        song_title text, 
        user_first_name text,
        user_last_name text,
        PRIMARY KEY ((user_id, session_id), item_in_session))
        WITH CLUSTERING ORDER BY (item_in_session ASC);
'''

song_table_create = '''
    CREATE TABLE IF NOT EXISTS song_library (
        song_title text, 
        user_id bigint,
        session_id bigint, 
        item_in_session int,
        user_first_name text,
        user_last_name text,
        PRIMARY KEY ((song_title), user_id, session_id, item_in_session)
    );
'''

#%% Tables insertion
session_table_insert = '''
    INSERT INTO session_library (
        session_id, 
        item_in_session, 
        artist_name, 
        song_title, 
        song_length) 
    VALUES (%s, %s, %s, %s, %s)
'''

user_table_insert = '''
    INSERT INTO user_library (
        user_id, 
        session_id, 
        item_in_session, 
        artist_name,
        song_title, 
        user_first_name, 
        user_last_name) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

song_table_insert = '''
    INSERT INTO song_library (
        song_title, 
        user_id,
        session_id, 
        item_in_session,
        user_first_name,
        user_last_name)
    VALUES (%s, %s, %s, %s, %s, %s)
'''

#%% Tables drop
session_table_drop = 'DROP TABLE IF EXISTS session_library'
user_table_drop = 'DROP TABLE IF EXISTS user_library'
song_table_drop = 'DROP TABLE IF EXISTS song_library'

#%% Queries grouping
create_table_queries = [session_table_create,
                        user_table_create,
                        song_table_create]

insert_table_queries = [session_table_insert,
                        user_table_insert,
                        song_table_insert]

drop_table_queries = [session_table_drop,
                      user_table_drop,
                      song_table_drop]
