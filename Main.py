from Chess_com_archive import archive_game_list, chess_matches
from chess_pos_processing import extract_png_df

archive_list = archive_game_list('lpsupi')
matches = chess_matches(archive_list,'lpsupi')
extract_png_df('lpsupi_archive.xlsx','lpsupi')