from folders_set_up import folder_creation
from Chess_com_archive import archive_game_list, chess_matches
from chess_pos_processing import extract_png_df
from config import chess_players

folder_creation()

'''for player in chess_players:

    archive_list = archive_game_list(player)
    matches = chess_matches(archive_list,player)
    '''
for player in chess_players:
    extract_png_df('{}_archive.xlsx'.format(player),player)