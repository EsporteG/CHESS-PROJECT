import boto3
import os
from dotenv import load_dotenv
import awswrangler as wr
import pandas as pd

load_dotenv() 
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
region_name = os.environ.get('region_name')

Bucket_name = 'raw-chess-players-games'

def upload_raw_files(Bucket_name,file_name):
    s3 = boto3.client(
    's3',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key= aws_secret_access_key
    )
    s3.create_bucket(Bucket=Bucket_name)
    s3.upload_file(
        Filename='raw_chess_players_data\lpsupi_archive.xlsx',
        Bucket=Bucket_name,
        Key='lpsupi_archive.xlsx'
    )

df = pd.read_excel('raw_chess_players_data\lpsupi_archive.xlsx')
session = boto3.Session(
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key= aws_secret_access_key
    )
s3 = session.resource('s3')

wr.s3.to_excel(df, 's3://raw-chess-players-games/lpsupi_archive2.xlsx', index=False)


