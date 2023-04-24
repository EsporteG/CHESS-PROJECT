import boto3
import os
from dotenv import load_dotenv
import awswrangler as wr
import pandas as pd
from sqlalchemy import create_engine

load_dotenv() 
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
region_name = os.environ.get('region_name')
postgres = os.environ.get('postgress_connection')

def check_bucket_existence(bucket_name):
    buckets_list=[]
    session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
        )
    s3 = session.resource('s3')
    for bucket in s3.buckets.all():
        buckets_list.append(bucket.name)
    if bucket_name in buckets_list:
        bucket_exists = True
    else:
        bucket_exists = False
    return bucket_exists

def upload_raw_files(Bucket_name, df, player_name):
    print(f"---Load raw data process started...")
    session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
        )
    s3 = session.resource('s3')
    if check_bucket_existence(Bucket_name) == False:
        s3.create_bucket(Bucket=Bucket_name,CreateBucketConfiguration={'LocationConstraint': region_name})
    else:
        pass
    wr.s3.to_csv(df, 's3://{}/{}_raw.csv'.format(Bucket_name,player_name), index=False)
    return print(f"---Load raw data process complete...")

def s3_key_list(Bucket_name):
    s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )

    files_metadata = s3_client.list_objects(Bucket=Bucket_name)['Contents']

    key_list = []

    for file in files_metadata:
        key_list.append(file["Key"])
    return key_list

def upload_pgn_files(Bucket_name, df, player_name):
    print(f"---Load PGN data process started...")
    session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
        )
    s3 = session.resource('s3')
    if check_bucket_existence(Bucket_name) == False:
        s3.create_bucket(Bucket=Bucket_name,CreateBucketConfiguration={'LocationConstraint': region_name})
    else:
        pass
    wr.s3.to_csv(df, 's3://{}/{}_pgn.csv'.format(Bucket_name,player_name), index=False)
    print(f"---Load PGN data process complete...")
    return print(f"--------------------------PGN Load Complete---------------------------")

def load_to_rds(mode='replace', df=None):
    db = create_engine(postgres)
    conn = db.connect()
    df=df
    with conn.begin() as connection:
        df.to_sql(
            name='game_data',
            con=conn,
            if_exists=mode,
            index=False,
            schema='public',
            method=None
        )

