from cassandra.cluster import Cluster
from cql_queries import create_table_queries, drop_table_queries


def create_keyspace():
    """Create and connect to the sparkifydb.

    Returns
    -------
    cluster : cassandra.Cluster
        One instance of this class will be created for each separate 
        Cassandra cluster.
    session : cassandra.Cluster.Session
        A collection of connection pools for each host in the cluster. It will
        be used to execute queries and statements using execute() method.
    """
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()

        session.execute('DROP KEYSPACE IF EXISTS music_library')
        session.execute('''
            CREATE KEYSPACE IF NOT EXISTS music_library
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        ''')
        session.set_keyspace('music_library')
    except Exception as e:
        print('Error while creating keyspace.')
        print(e)

    return cluster, session


def drop_tables(session):
    """Drop each table using the queries in `drop_table_queries` list.

    Parameters
    ----------
    session : cassandra.Cluster.Session
        A collection of connection pools for each host in the cluster. It will
        be used to execute queries and statements using execute() method.
    """
    try:
        for query in drop_table_queries:
            session.execute(query)
    except Exception as e:
        print('Error while droping tables.')
        print(e)


def create_tables(session):
    """Create each table using the queries in `create_table_queries` list. 

    Parameters
    ----------
    session : cassandra.Cluster.Session
        A collection of connection pools for each host in the cluster. It will
        be used to execute queries and statements using execute() method.
    """
    try:
        for query in create_table_queries:
            session.execute(query)
    except Exception as e:
        print('Error while creating tables')
        print(e)


def main():
    """Drop (if exists) and creates the music_library keyspace."""
    cluster, session = create_keyspace()

    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
