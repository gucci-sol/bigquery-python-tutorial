from google.cloud import bigquery

def query_stackoverflow():
  # 認証を行いBigQuery APIに接続するようにクライアントを初期化。
  client = bigquery.Client()

  query_job = client.query(
    """
    SELECT
      CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, view_count
    FROM
      `bigquery-public-data.stackoverflow.posts_questions`
    WHERE
      tags LIKE '%google-bigquery%'
    ORDER BY
      view_count DESC
    LIMIT
      10
    """
  )

  results = query_job.result()

  for row in results:
    print("{} : {} views".format(row.url, row.view_count))

if __name__ == "__main__":
  query_stackoverflow()