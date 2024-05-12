"""
This script connects to a PostgreSQL database using psycopg2 and performs
several operations.

By using a password stored in secret file, it creates a connection string and
connects to the database. Then, it creates a table in the database by executing
a SQL script from a file. Next, it imports data from a CSV file into the
database using another SQL script. Finally, it executes a series of test
queries from a third SQL script and prints the results.

The database connection and cursor are closed and deallocated after each
operation.
"""
__author__ = "Artyom, Almaz, Timofey"
__version__ = "1.0.1"

from pprint import pprint

import os

import psycopg2 as psql

file = os.path.join("secrets", ".psql.pass")
DB_PASSWORD = None
with open(file, "r", encoding="utf-8") as file:
    DB_PASSWORD = file.read().rstrip()

CONN_STRING = "host=hadoop-04.uni.innopolis.ru port=5432 user=team16"\
              f" dbname=team16_projectdb password={DB_PASSWORD}"

with psql.connect(CONN_STRING) as conn:
    cur = conn.cursor()
    print('Creating table...')
    with open(os.path.join("sql", "create_table.sql"),
              encoding="utf-8") as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    print('Importing data...')
    with open(os.path.join("sql", "import_data.sql"),
              encoding="utf-8") as file:
        commands = file.readlines()
        with open(os.path.join("data", "2019-Oct.csv"),
                  "r", encoding="utf-8") as data:
            cur.copy_expert(commands[0], data)
    conn.commit()

    with open(os.path.join("sql", "import_data_small.sql"),
              encoding="utf-8") as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    pprint(conn)
    cur = conn.cursor()
    print('Testing database...')
    with open(os.path.join("sql", "test_database.sql"),
              encoding="utf-8") as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            pprint(cur.fetchall())
