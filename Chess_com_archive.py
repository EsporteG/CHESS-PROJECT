import pandas as pd
import requests
from S3_functions import upload_raw_files
from dotenv import load_dotenv
import os

load_dotenv() 

Bucket_raw = os.environ.get('Bucket_raw')
Bucket_pgn = os.environ.get('Bucket_pgn')

def archive_game_list(player_name):
    print(f"---------------------EL {player_name} Games---------------------")
    print(f"---Extraction process started...")
    url = f'https://api.chess.com/pub/player/{player_name}/games/archives'
    game_list = requests.get(url).json()['archives']
    return game_list

def chess_matches(chess_players):
    for player in chess_players:
        archive_list = archive_game_list(player)
        df = []
        for matches in archive_list:
            matches = pd.json_normalize(requests.get(matches).json()["games"])
            matches = matches.squeeze()
            df.append(matches)
            df_final = pd.concat(df)
        print(f"---Extraction process complete...")
        upload_raw_files(Bucket_name=Bucket_raw,df=df_final,player_name=player) 
    return print(f"--------------------------EL Complete---------------------------")  