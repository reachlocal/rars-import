import os
import csv
import MySQLdb
import MySQLdb.cursors as cursors
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta
import threading

def flush_to_file(table, data, header, storage_client, bq_client, google_folder, bucket_name, bq_dataset):
  now = datetime.now().strftime('%Y%m%dT%H%M%S')
  filename = table + '-' + now + '.csv'
  filepath = os.path.expanduser(os.path.join('', filename))
  print(f'Writing to file {filepath}')

  with open(filepath, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(header)
    writer.writerows(data)
  thread = threading.Thread(target = upload_and_load_file, args = (table, storage_client, bq_client, google_folder, bucket_name, bq_dataset, filename, filepath))
  thread.start()


def upload_and_load_file(table, storage_client, bq_client, google_folder, bucket_name, bq_dataset, filename, filepath):
  bucket = storage_client.get_bucket(bucket_name)

  blob_path = f'{google_folder}/{filename}'
  blob = bucket.blob(blob_path)
  blob.upload_from_filename(filepath)
  print(f'Uploaded to {blob_path}')
  os.remove(filepath)

  table_id = f'{bucket_name}.{bq_dataset}.{table}'
  uri = f'gs://{bucket_name}/{blob_path}'

  job_config = bigquery.LoadJobConfig(
              autodetect=False,
              source_format=bigquery.SourceFormat.CSV,
              skip_leading_rows=1,
              max_bad_records=1)

  print(f'Loading {uri} into {table_id}')
  load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)

def get_period_column(columns_result):
    date_columns = list(filter(lambda x: x[1] == 'date', columns_result))
    datetime_columns = list(filter(lambda x: x[1] == 'datetime', columns_result))
    period_column = date_columns if len(date_columns) > 0 else datetime_columns
    valid_period_column = period_column if len(period_column) == 1 else []
    filtered_period_column = list(filter(lambda x: x[0] != 'last_modified', valid_period_column))
    return filtered_period_column[0] if len(filtered_period_column) == 1 else None

def main():
    mydb = MySQLdb.connect(
        host = os.environ['MYSQL_HOST'],
        user = os.environ['MYSQL_USER'],
        password = os.environ['MYSQL_PASSWORD'],
        cursorclass = cursors.SSCursor,
        charset='utf8'
    )
    schema = os.environ['MYSQL_SCHEMA']

    storage_client = storage.Client()
    bq_client = bigquery.Client(project = os.environ['GOOGLE_PROJECT'])

    tables_cursor = mydb.cursor()
    tables_cursor.execute(f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{schema}' ORDER BY TABLE_NAME ASC")
    tables = list(map(lambda x: x[0], tables_cursor))
    print(f'Tables: {tables}')


    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    google_folder = f'{os.environ["GCS_FOLDER"]}/{yesterday}'
    bucket_name = os.environ['GCS_BUCKET']
    bq_dataset = os.environ['BQ_DATASET']

    mycursor = mydb.cursor()
    for table in tables:
        header_cursor = mydb.cursor()
        header_cursor.execute(f'SHOW COLUMNS FROM `{schema}`.{table}')
        columns_result = header_cursor.fetchall()
        header = list(map(lambda x: x[0].replace('.', '_').replace('-', '_').replace('+', '_plus'), columns_result))
        period_column = get_period_column(columns_result)

        query = f'SELECT * FROM  `{schema}`.{table}'
        if period_column is not None:
          if period_column[1] == 'date':
            query = query + f' WHERE {period_column[0]} = \'{yesterday}\''
          else:
            query = query + f' WHERE CAST({period_column[0]} AS DATE) = \'{yesterday}\''
        else:
          truncate_query = f'DELETE FROM `{bq_dataset}`.{table} WHERE 1=1'
          query_job = bq_client.query(truncate_query)
          result = query_job.result()
          print(f'Truncated table `{bq_dataset}`.{table}: {result}')
        print(f'Retrieving data for {table}: {query}')
        mycursor.execute(query)

        rows = []
        for row in mycursor:
            entry = list(map(lambda x: x.replace('\n', '\\n').replace('\r', '\\r').replace('\0', '').replace('\x00', '') if isinstance(x, str) else x, row))
            rows.append(entry)
            if len(rows) >= 1000000:
                flush_to_file(table, rows, header, storage_client, bq_client, google_folder, bucket_name, bq_dataset)
                rows = []
        flush_to_file(table, rows, header, storage_client, bq_client, google_folder, bucket_name, bq_dataset)
        print(f'Saved data for {table}')

main()
