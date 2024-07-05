#Task 3

from sqlalchemy import create_engine
import pandas as pd

source_db_url = "postgresql+psycopg2://username:password@source_host:source_port/source_db"
dest_db_url = "postgresql+psycopg2://username:password@dest_host:dest_port/dest_db"

source_engine = create_engine(source_db_url)
dest_engine = create_engine(dest_db_url)

with source_engine.connect() as source_conn, dest_engine.connect() as dest_conn:
    tables = source_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()
    tables = [table[0] for table in tables]

    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", source_conn)
        df.to_sql(table, dest_conn, index=False, if_exists='replace')
