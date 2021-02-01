#!/usr/bin/env python3
import os
import psycopg2
import json
import shutil
from config import config
from datetime import datetime
from random import randint, choice, uniform
from string import ascii_uppercase

# Class that creates simulative data for feeding sinker


class Simulation:
    def __init__(self):
        # key and values simulation
        self.key = f'{choice(ascii_uppercase)}{randint(100,999)}'
        self.value = round(uniform(1, 100), 1)
# random ts simulation

    def random_ts(self):
        timestamp = randint(0000000000, 9999999999)
        readable_time = datetime.fromtimestamp(timestamp)
        date = readable_time.strftime('%Y-%d-%m %H:%M:%S')
        z_sings = ['-', '+']
        ts = f'{date}.{randint(100000, 999999)}{choice(z_sings)}0{randint(0,9)}:00'
        return ts
# dictionary formed data

    def generate_data(self):
        generate_data = {"key": f'{self.key}',
                         "value": f'{self.value}', 'ts': f'{self.random_ts()}'}
        return generate_data

# use simulation for creating data sources


def data_json_feeder(count):
    file_data = []
    for i in range(count):
        source = Simulation()
        file_data.append(source.generate_data())
    return json.dumps(file_data)

# creating files(array of jsons)


def sinker():
    # list all files from ingestion directory in list
    files = os.listdir('./ing_a')

    # DB connection and config
    conn = None
    params = config()

    # test if no files no need for work
    if len(files) < 1:
        print("There is nothing for ingest!")
        exit

    try:
        conn = psycopg2.connect(**params)
        psql_cursor = conn.cursor()

        for j_file in files:
            with open(f'ing_a/{j_file}') as json_file:
                data = json.load(json_file)
                for d in data:
                    # sanitized data
                    jon = json.dumps(d)
                    key = d['key']
                    value = d['value']
                    ts = d['ts']
                    # console prin and insert
                    print(f'Load: {jon}')
                    insrt = f'INSERT INTO inf_messages(KEY, VALUE, TS, ROW_JSON)VALUES(\'{key}\', {value}, \'{ts}\', \'{jon}\')'
                    psql_cursor.execute(insrt)
                    conn.commit()
        psql_cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:  # else broke earlyer
            conn.close()


def move_ingested_files():
    files = os.listdir('./ing_a')
    source_dir = '/home/raddy/PseudoETL/ing_a/'
    target_dir = '/home/raddy/PseudoETL/archive/'

    for file_name in files:
        shutil.move(os.path.join(source_dir, file_name), target_dir)


# give name of the json file
file_name = Simulation().key
test = data_json_feeder(1000)
write_file = open(f'ing_a/{file_name}.json', 'w')
write_file.write(test)
write_file.close()
sinker()
move_ingested_files()
