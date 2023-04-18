import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv() 
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
    df_final = df_final.iloc[:, :-5]

    print(f"---Transform: Extract PGN data process complete...")
    return df_final