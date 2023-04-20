# DE-PROJECT

Data Engineer project

# Goal

Create an full DE project to get all games from selected players of Chess.com, process the data to use on a PBI DashBoard.

# Steps

1. Get raw data from Chess.com using their public API
2. Process/ Clean the data
3. Create a scheduler for those python scripts using crontab/ airflow/ lambda
4. Upload the data into a S3 and RDS on AWS
5. Consume this data on PBI

#How to run the main code:

1. Set up your .env file, with the AWS credentials and Buckets names.
2. Install all dependencies.
3. setup the chess player on the config.py
4. Run the code and make your analysis

Any questions?
Contact me: https://www.linkedin.com/in/gustavoesantos/
