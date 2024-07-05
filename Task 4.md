#Task 4

from sqlalchemy import create_engine
import pandas as pd

source_db_url = "postgresql+psycopg2://username:password@source_host:source_port/source_db"
dest_db_url = "postgresql+psycopg2://username:password@dest_host:dest_port/dest_db"

source_engine = create_engine(source_db_url)
dest_engine = create_engine(dest_db_url)

tables_columns = {
    "table1": ["column1", "column2"],
    "table2": ["column3", "column4"]
}

with source_engine.connect() as source_conn, dest_engine.connect() as dest_conn:
    for table, columns in tables_columns.items():
        df = pd.read_sql(f"SELECT {', '.join(columns)} FROM {table}", source_conn)
        df.to_sql(table, dest_conn, index=False, if_exists='replace')
