import configparser
import psycopg2
from sql import create_table_queries, drop_table_queries
from sys import exit


def drop_tables(cur, conn):
    """Drop all tables using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection established.")
    except Exception as e:
        print("Something went wrong trying to connect to the cluster:")
        print(e)
        exit()

    drop_tables(cur, conn)
    print("All tables droped.")
    create_tables(cur, conn)
    print("All tables created.")

    conn.close()


if __name__ == "__main__":
    main()