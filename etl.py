import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function is used to stage data from S3 into tables in Redshift Cluster."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This function is used to transform data from staging tables into analytical tables (star schema)."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to call the functions which will stage tables from S3 then transform them into analytical tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()