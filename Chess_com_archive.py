import pandas as pd
import requests


def archive_game_list(player_name):
    print(f"---------ETL {player_name} Games---------")
    print(f"---Extraction process started...")
    url = f'https://api.chess.com/pub/player/{player_name}/games/archives'
    game_list = requests.get(url).json()['archives']
    return game_list

def chess_matches(archive_url,player_name):
    df = []
    for matches in archive_url:
        matches = pd.json_normalize(requests.get(matches).json()["games"])
        matches = matches.squeeze()
        df.append(matches)
        df_final = pd.concat(df)
    print(f"---Extraction process complete...")    
    return df_final, player_name