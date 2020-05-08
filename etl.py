import configparser
import psycopg2
import os
from sql_queries import insert_table_queries, copy_table_queries

from dotenv import load_dotenv
load_dotenv()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read_file(open('vpconfig.cfg'))
    host = config.get('RED', 'red_endpoint')

    db_name = os.getenv('red_db')
    db_user = os.getenv('red_db_user')
    db_password = os.getenv('red_db_password')
    db_port = os.getenv('red_port')

    conn = psycopg2.connect(
        f'host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}')
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
