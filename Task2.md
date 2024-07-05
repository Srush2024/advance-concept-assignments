#Task 2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa
import fastavro

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'export_db_to_file',
    default_args=default_args,
    description='Export DB to CSV, Parquet, and Avro',
    schedule_interval=timedelta(days=1),
)

def export_data():
    db_url = "postgresql+psycopg2://username:password@localhost:5432/database_name"
    engine = create_engine(db_url)
    query = "SELECT * FROM your_table"
    df = pd.read_sql(query, engine)

    df.to_csv('output.csv', index=False)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'output.parquet')

    records = df.to_dict(orient='records')
    schema = {
        "type": "record",
        "name": "MyRecord",
        "fields": [{"name": col, "type": "string"} for col in df.columns]
    }
    with open('output.avro', 'wb') as out:
        fastavro.writer(out, schema, records)

export_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data,
    dag=dag,
)

export_task
