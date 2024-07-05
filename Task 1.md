# advance-concept-assignments
#Task 1

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa
import fastavro

# Database connection details
db_url = "postgresql+psycopg2://username:password@localhost:5432/database_name"

# Connect to the database
engine = create_engine(db_url)
query = "SELECT * FROM your_table"

# Fetch data from database
df = pd.read_sql(query, engine)

# Export to CSV
df.to_csv('output.csv', index=False)

# Export to Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, 'output.parquet')

# Export to Avro
records = df.to_dict(orient='records')
schema = {
    "type": "record",
    "name": "MyRecord",
    "fields": [{"name": col, "type": "string"} for col in df.columns]
}
with open('output.avro', 'wb') as out:
    fastavro.writer(out, schema, records)
