import sqlalchemy
from sqlalchemy.sql import text
import os
import json

def get_sql_client(database_name):
    
    f = open('secrets.json')
    secrets = json.load(f)
    print(secrets)
    
    db_user = secrets['db_user']
    db_pass = secrets['db_password']
    db_name = database_name

    if os.getenv('GAE_ENV', '').startswith('standard'):
        # Production in the standard environment
        db_socket_dir = "/cloudsql"
        cloud_sql_connection_name = "rooscrape:europe-west2:foodhoover-web"
        
        pool = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername="postgresql+psycopg2",
                username=db_user,  # e.g. "my-database-user"
                password=db_pass,  # e.g. "my-database-password"
                database=db_name,  # e.g. "my-database-name"
                host = db_socket_dir+"/"+cloud_sql_connection_name
            ),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800
        )

    else:
        # Local execution.
        db_hostname = '127.0.0.1'
        db_port=5432

        pool = sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL(
                    drivername="postgresql+psycopg2",
                    username=db_user,  # e.g. "my-database-user"
                    password=db_pass,  # e.g. "my-database-password"
                    host=db_hostname,  # e.g. "127.0.0.1"
                    port=db_port,  # e.g. 5432
                    database=db_name  # e.g. "my-database-name"
                )
            )

    pool.dialect.description_encoding = None

    return pool
