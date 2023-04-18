import os 
from config import folders

def folder_creation():
    for folder in folders:
        try: 
            os.mkdir(folder) 
        except OSError as error: 
            pass

    return print ("Folders set up done!") 