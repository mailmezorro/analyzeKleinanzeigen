import os
import logging
import sys
import scrapeRealEstate.scripts
import scrapeRealEstate.config
import scrapeRealEstate.scripts.config_utils as config_utils
import psycopg2
import pandas as pd
from  pathlib import Path


def main():
    workdir = Path(__file__).parent

    # Init Database
    config_db = config_utils.load_config_file(workdir/"scrapeRealEstate/config/db_config.json")
    
    try:
        conn = psycopg2.connect(
            dbname=config_db.get('dbname'),
            user=config_db.get('user'),
            password=config_db.get('password'),  
            host=config_db.get('host'),
            port=config_db.get('port')
        )
        print("Connection successful!")
        
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return
    
    # Pull data from the database
    try:
        df = pd.read_sql_query("SELECT * FROM kleinanzeigen_immobilien", conn)
        df.to_csv(workdir / "kleinanzeigen_immobilien.csv", index=False) 
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        conn.close()  
    return 0
    
if __name__ == '__main__':
    main()