import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import time


def drop_tables(cur, conn):
    """Executes query to drop tables"""
    
    print('Dropping tables if they exist...')
    start = time.time()
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    
    print('Done')
    end = time.time()
    runtime = end - start
    print('runtime (seconds): ',runtime)
    print('\n\n')


def create_tables(cur, conn):
    """Executes query to create tables"""
    
    print('Creating tables if they do not exist...')
    start = time.time()
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('Done')
    end = time.time()
    runtime = end - start
    print('runtime (seconds): ',runtime)
    print('\n\n')


def main():
    """main function that calls the drop_tables and create_tables functions"""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()