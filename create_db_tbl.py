
import psycopg2
import constants

# Establish connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=constants.DB_NAME,
    user=constants.DB_USER,
    password=constants.DB_PASSWORD,
    host=constants.DB_HOST,
    port=constants.DB_PORT
)

cur = conn.cursor()

# CREATE TABLE SQL command
create_table_query = """
    CREATE TABLE IF NOT EXISTS postgres.public.dog_lover_data (
        id                SERIAL PRIMARY KEY
        ,name             varchar(255)
        ,bred_for         varchar(150)
        ,breed_group      varchar(150)
        ,weight           varchar(20)
        ,height           varchar(20)
        ,life_span        varchar(20)
        ,temperament      varchar(150)
        ,origin           varchar(20)
        ,image            varchar(255)
        ,weight_min        int
        ,weight_max        int
        ,height_min        int
        ,height_max        int
        ,life_span_min     int
        ,life_span_max     int
    );
"""

cur.execute(create_table_query)

# Commit and close the transaction
conn.commit()
cur.close()
conn.close()
