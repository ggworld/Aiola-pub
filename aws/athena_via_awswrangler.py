import awswrangler as wr
import pandas as pd
import boto3

boto3.setup_default_session(region_name="eu-west-1")
df = wr.athena.read_sql_query("SELECT 1 as my_t", database="default",s3_output='s3://my-athena-tmp')
print(df)
      
