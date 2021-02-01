import os
import psycopg2
import json
from config import config
from datetime import datetime
from random import randint, choice, uniform
from string import ascii_uppercase

'''
class Simulation -> create simulation data in demanded format, array of jsons file;

def sinker -> reads, json file retriev in postges and print it;

'''


class Simulation:
    def __init__(self, count):
        # count of jsons in the file
        self.count = count
# random key

    def rand_key(self):
        key = f'{choice(ascii_uppercase)}{randint(100,999)}'
        return key
# random value

    def rand_value(self):
        value = round(uniform(1, 100), 1)
        return value
# random timestamp

    def rand_ts(self):
        timestamp = randint(0000000000, 9999999999)
        readable_time = datetime.fromtimestamp(timestamp)
        date = readable_time.strftime('%Y-%d-%m %H:%M:%S')
        z_sings = ['-', '+']
        ts = f'{date}.{randint(100000, 999999)}{choice(z_sings)}0{randint(0,9)}:00'
        return ts
# dictionary formed data

    def generate_data(self):
        generate_data = {"key": f'{self.rand_key()}',
                         "value": f'{self.rand_value()}', 'ts': f'{self.rand_ts()}'}
        return generate_data
# json array

    def data_json_feeder(self):
        file_data = []
        for i in range(self.count):
            file_data.append(self.generate_data())
        return json.dumps(file_data)
# file with array self.count jsons and name rand_key().json

    def write_file(self):
        # give name of the json file create file
        file_name = f'{self.rand_key()}.json'
        new_file = self.data_json_feeder()
        write_file = open(file_name, 'w')
        write_file.write(new_file)
        write_file.close()
        return file_name


# print data and write

def etl(file):
    # load config
    conn = None
    params = config()

    try:
        # db connect
        conn = psycopg2.connect(**params)
        psql_cursor = conn.cursor()

        # extract data
        with open(file) as json_file:
            data = json.load(json_file)
            for d in data:
                # sanitized data
                load_json = json.dumps(d)
                key = d['key']
                value = d['value']
                ts = d['ts']
                # console print and insert in DB
                print(f'Load: {load_json}')
                insrt = f'INSERT INTO inf_messages(KEY, VALUE, TS, ROW_JSON)VALUES(\'{key}\', {value}, \'{ts}\', \'{load_json}\')'
                psql_cursor.execute(insrt)
                conn.commit()
        psql_cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


etl(Simulation(1000).write_file())
