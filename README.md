# DE-PROJECT
Data Engineer project

# Goal
Create an full DE project to get all games from selected players of Chess.com, process the data to use on a PBI DashBoard.

# Steps

1. Get raw data from Chess.com using their public API
2. Process/ Clean the data
3. Create a scheduler for those python scripts using crontab/ airflow/ lambda
4. Upload the bata into a DB using AWS or Azure
5. Consume this data on PBI

#Depencies to install:
1. pandas
2. os
3. requests

#How to run the main code:
1. Inside the DE_PROJECT older create 2 new empty folder called: 
  1.1 png_df
  1.2 raw_chess_player_data
2. Choose the chess player name (needs to be the nickname of Chess.com) and substitute all lpsupi in the main.py file
3. Run the code and make your analysis

Any questions? 
Contact me: https://www.linkedin.com/in/gustavoesantos/
