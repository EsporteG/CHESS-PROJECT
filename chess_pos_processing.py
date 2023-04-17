import pandas as pd
import os

def extract_png_df(raw_file, player_name_pos):
    print("PGN data extraction started...")
    df = pd.read_excel(f'./raw_chess_players_data/{raw_file}')
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
    os.chdir('./pgn_players')
    df_final.to_excel(f'{player_name_pos}_pgn.xlsx')
    os.chdir('../')
    return print(f"PGN data of {player_name_pos} exported!")