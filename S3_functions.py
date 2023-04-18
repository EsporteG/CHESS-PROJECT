import boto3
import os
from dotenv import load_dotenv
import awswrangler as wr
import pandas as pd

load_dotenv() 
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
region_name = os.environ.get('region_name')

def upload_raw_files(Bucket_name, df, player_name):
    session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
        )
    s3 = session.resource('s3')
    s3.create_bucket(Bucket=Bucket_name)
    wr.s3.to_excel(df, 's3://{}/{}_raw.xlsx'.format(Bucket_name,player_name), index=False)