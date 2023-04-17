import os 
from config import folders
    
# path 

    
# Create the directory 
# 'GeeksForGeeks' in 
# '/home / User / Documents' 
def folder_creation():
    for folder in folders:
        try: 
            os.mkdir(folder) 
        except OSError as error: 
            pass

    return print ("Folders set up done!") 