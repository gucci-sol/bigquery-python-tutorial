from google.cloud import bigquery

client = bigquery.Client()

def main():
  project_id: str = client.project # Get Project ID
  dataset_id: str = 'babynames'
  table_id: str = 'names_20144'
  dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
  table_ref = dataset_ref.table(table_id)
  table = client.get_table(table_ref)
  print("Table {} contains {} columns".format(table_id, len(table.schema)))

  # Configuration options for load jobs.
  job_config: bigquery.job.LoadJobConfig = bigquery.LoadJobConfig()

  # Action that occurs if the destination table already exists.
  # **OPTION**
  # bigquery.WriteDisposition.WRITE_APPEND : If the table already exists, BigQuery appends the data to the table.
  # bigquery.WriteDisposition.WRITE_EMPTY : If the table already exists and contains data, a ‘duplicate’ error is returned in the job result.
  # bigquery.WriteDisposition.WRITE_TRUNCATE : If the table already exists, BigQuery overwrites the table data.
  # ****
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
  
  # Specifies updates to the destination table schema to allow as a side effect of the load job.
  # **OPTION**
  # bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION : Allow adding a nullable field to the schema.
  # bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION : Allow relaxing a required field in the original schema to nullable.
  # ****
  # デフォルトでは、必須フィールドの追加のみ可能？ →　必須フィールドは追加できない
  job_config.schema_update_options = [
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
  ]

  # Schema of the destination table.
  # 空テーブルに追加する場合は、必須フィールドを設定可能。
  # すでにテーブルにデータが存在する場合は、NULL許容の追加が可能。
  # Null許容から必須への変更は不可。
  # 配列の場合は、REPEATEDを追加できる？
  job_config.schema = [
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
  ]

  # File format of the data.
  job_config.source_format = bigquery.SourceFormat.CSV

  # Number of rows to skip when reading data (CSV only).
  job_config.skip_leading_rows = 1

  filepath: str = './sample.csv'
  with open(filepath, 'rb') as source_file:
    job = client.load_table_from_file(
      source_file,
      table_ref,
      location='US',
      job_config=job_config
    )
  job.result()
  print(
    "Load {} rows into {}:{}".format(
      job.output_rows, dataset_id, table_ref.table_id
    )
  )

  table = client.get_table(table)
  print("Table {} now contains {} columns.".format(table_id, len(table.schema)))



if __name__ == "__main__":
  main()