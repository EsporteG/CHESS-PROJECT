
from Chess_com_archive import archive_game_list, chess_matches
from chess_pos_processing import extract_png_df
from config import chess_players
from S3_functions import upload_raw_files, upload_pgn_files
from dotenv import load_dotenv
import os

load_dotenv() 

Bucket_raw = os.environ.get('Bucket_raw')
Bucket_pgn = os.environ.get('Bucket_pgn')

for player in chess_players:

    archive_list = archive_game_list(player)
    matches,player = chess_matches(archive_list,player)
    upload_raw_files(Bucket_name=Bucket_raw,df=matches,player_name=player)
    pgn_df = extract_png_df(matches)
    upload_pgn_files(Bucket_name=Bucket_pgn,df=pgn_df,player_name=player) 