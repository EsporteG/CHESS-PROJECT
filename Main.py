
from Chess_com_archive import chess_matches
from chess_pos_processing import extract_png_df
from config import chess_players
from S3_functions import upload_pgn_files

chess_matches(chess_players)

'''pgn_df = extract_png_df(matches)
upload_pgn_files(Bucket_name=Bucket_pgn,df=pgn_df,player_name=player) '''