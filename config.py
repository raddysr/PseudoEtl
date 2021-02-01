#!/usr/bin/python3
from configparser import ConfigParser

def config(filename='database.ini', section='postgresql'):
    # create parser
    parser = ConfigParser()
    # read config
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('{0} not found in the {1}'.format(section, filename))

    return db
