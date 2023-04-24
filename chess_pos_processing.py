import pandas as pd
from dotenv import load_dotenv
import os
from config import pgn_columns,pgn_new_columns
from S3_functions import s3_key_list, load_to_rds
import awswrangler as wr
import boto3
from sqlalchemy import create_engine

load_dotenv() 

region_name = os.environ.get('region_name')
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
columns_pgn = os.environ.get('columns_pgn')
Bucket_raw = os.environ.get('Bucket_raw')
postgres = os.environ.get('postgress_connection')

def extract_png_df(df):
    print(f"---Transform: Extract PGN data process started...")
    df = df.dropna(subset=['pgn'])
    df = list(df['pgn'])
    df = list(map(lambda value: value.split('\n\n')[0].split('\n'), df))
    df_f = []

    for png in df:
        df = list(map(lambda value: value.strip("[").strip("]").strip('\s').rstrip('"').replace(" ","").split('"'), png))
        df_f.append(df)
        i = 1

    df_final = pd.DataFrame()
    for pgn_pro in df_f:
        df = pd.DataFrame(pgn_pro)
        df = df.transpose()
        df = df.rename(columns=df.iloc[0]).drop(df.index[0])
        df['match id'] = i
        i=i+1
        df_final = pd.concat([df_final,df])
    df_final = df_final[pgn_columns]
    new_columns_name = dict(zip(pgn_columns, pgn_new_columns))
    df_final = df_final.rename(new_columns_name)

    print(f"---Transform: Extract PGN data process complete...")
    return df_final

def extract_load_pgn():
    files_list = s3_key_list(Bucket_raw)
    n_files = len(files_list)
    i = 0
    s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )
    for file in files_list:
        response = s3_client.get_object(Bucket=Bucket_raw, Key=file)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Downloading {file}")
            df = pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

        pgn_df = extract_png_df(df)
        print("---PGN extract from {}... ".format(file))

        if i == 0:
            load_to_rds(mode='replace', df=pgn_df)
            print("---PGN data load on RDS...")
                
        else:
            load_to_rds(mode='append', df=pgn_df)
        i+=1