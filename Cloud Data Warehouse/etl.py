import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Iterate overload queries queries and execute each querie. 
    each query will : copy  the data from the S3 buckets to the stage tables. 
    """
    for query in copy_table_queries:
        #print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Iterates over insert queries list. insert data to the related dimensions from the 
    staging tables.
    """
    for query in insert_table_queries:
        #print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()