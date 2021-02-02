import os
import psycopg2
import json
from config import config
from datetime import datetime
from random import randint, choice, uniform
from string import ascii_uppercase

'''
class Source -> create simulation data in demanded format, array of jsons file;

def sinker -> reads, json file retriev in postges and print it;

'''


class Source:
    def __init__(self, count=1, type='simulation'):
        # count of jsons in the file
        self.count = count
        # if is simulation(default or explicite) will generate a single message anything else will generate file with array of self.count jsons
        self.type = type

# generate random key

    def rand_key(self):
        key = f'{choice(ascii_uppercase)}{randint(100,999)}'
        return key

# generate random value

    def rand_value(self):
        value = round(uniform(1, 100), 1)
        return value

# generate random timestamp

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

# create json or json array

    def data_json_feeder(self):
        if self.type == 'simulation':
            return json.dumps(self.generate_data())
        else:
            file_data = []
            for i in range(self.count):
                file_data.append(self.generate_data())
            return json.dumps(file_data)

# file with array self.count jsons and name rand_key().json

    def write_file(self):
            file_name = f'{self.rand_key()}.json'
            new_file = self.data_json_feeder()
            write_file = open(file_name, 'w')
            write_file.write(new_file)
            write_file.close()
            return file_name

# extract data, transofrme it in a python structure and load it console and PostreSQL

def etl(source_output):
    # load config
    conn = None
    params = config()

    try:
        # db connect
        conn = psycopg2.connect(**params)
        psql_cursor = conn.cursor()

        # extract data
        if os.path.exists(source_output):
            with open(source_output) as json_file:
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
        else:
            load_json = json.loads(source_output)
            key = load_json['key']
            value = load_json['value']
            ts = load_json['ts']
            # console print and insert in DB
            print(f'Load: {source_output}')
            insrt = f'INSERT INTO inf_messages(KEY, VALUE, TS, ROW_JSON)VALUES(\'{key}\', {value}, \'{ts}\', \'{source_output}\')'
            psql_cursor.execute(insrt)
            conn.commit()
        psql_cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# load from a single message source

etl(Source().data_json_feeder())

# read and load a file(array with 1k jsons)

etl(Source(1000, 'file').write_file())
