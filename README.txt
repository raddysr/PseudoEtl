-- etl.py -> psuedo ETL program interface(run it 'python3 elt.py')

-- config.py -> load configuration from database.ini(loaded in etl.py so no need to touch it or run it)

-- not buildin used for connection to PostgreSQL -> psycopg2

-- database configuration config.py uses database.ini
 
-- please run commands via PSQL to have database required for testing the program:

/*

CREATE DATABASE pseudo_etl;
CREATE USER adastra WITH ENCRYPTED PASSWORD '@dmin123';
GRANT ALL PRIVILEGES ON DATABASE pseudo_etl TO adastra;
\c pseudo_etl
CREATE TABLE inf_messages (
   KEY VARCHAR NOT NULL,
   VALUE REAL NOT NULL,
   TS VARCHAR NOT NULL,
   ROW_JSON JSON NOT NULL
);
GRANT ALL PRIVILEGES ON TABLE inf_messages TO adastra;
*/
