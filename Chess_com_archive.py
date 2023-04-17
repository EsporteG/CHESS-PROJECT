import pandas as pd
import requests
import os

def archive_game_list(player_name):
    print(f"Extraction of {player_name} process started...")
    url = f'https://api.chess.com/pub/player/{player_name}/games/archives'
    game_list = requests.get(url).json()['archives']
    print("All game list storaged!")
    return game_list

def chess_matches(archive_url,player_name):
    print("Games extraction started..")
    df = []
    for matches in archive_url:
        matches = pd.json_normalize(requests.get(matches).json()["games"])
        matches = matches.squeeze()
        df.append(matches)
        df_final = pd.concat(df)
    os.chdir('./raw_chess_players_data')
    df_final.to_excel(f'{player_name}_archive.xlsx')
    print(f"All games of {player_name} exported!")
    os.chdir('../')
    return 
def chess_matches(archive_url,player_name):
    print("Games extraction started..")
    df = []
    for matches in archive_url:
        matches = pd.json_normalize(requests.get(matches).json()["games"])
        matches = matches.squeeze()
        df.append(matches)
        df_final = pd.concat(df)
    os.chdir('./raw_chess_players_data')
    df_final.to_excel(f'{player_name}_archive.xlsx')
    print(f"All games of {player_name} exported!")
    os.chdir('../')
    return 