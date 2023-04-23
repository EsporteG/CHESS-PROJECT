import pandas as pd
from dotenv import load_dotenv
import os
from config import pgn_columns
from S3_functions import s3_key_list
import awswrangler as wr

load_dotenv() 

region_name = os.environ.get('region_name')
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
columns_pgn = os.environ.get('columns_pgn')

def extract_png_df(df):
    print(f"---Transform: Extract PGN data process started...")
    df = df
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

    print(f"---Transform: Extract PGN data process complete...")
    return df_final

def tranform_load_pgn(files):
    s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )
    response = s3_client.get_object(Bucket=Bucket_name, Key=Key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    return df