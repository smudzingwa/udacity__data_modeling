import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_files(filepath):
    '''
    Returns a list of files in the provided file path

            Parameters:
                    a filepath (str): This is the file path 

            Returns:
                    all_files (list): This is a list of filenames in the provided file path
    '''    
    try:
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files :
                all_files.append(os.path.abspath(f))
    except:
        print(f"There was an error getting the files in the provided file path: {traceback.format_exec()}") 
        all_files = []
    
    return all_files

def process_song_data(cur, conn, file_path):
    '''
    Loads song_data from the files and saves it in the DB

            Parameters:
                    a cur (psycopg2.extensions.cursor): A cursor object to run operations on the postgress DB
                    b conn (psycopg2.extensions.connection): A connection object to connect to the postgress DB
                    c file_path (str): This is the file path where the song_data is saved 

            Returns:
                    None
    '''
        
    try:
        print("Starting to process song data")
        #getting the list of songfiles
        song_files = get_files(file_path)
        song_files_df = pd.DataFrame(song_files, columns=['file_path'])

        #removing files from notebook checkpoints
        song_files_df = song_files_df[~song_files_df['file_path'].str.contains('checkpoint')]

        print(f"\t --> Got {len(song_files_df)} song_data files")

        #loading the data
        print(f"\t --> Loading the data from the song_data files")
        combined_song_data_df = pd.DataFrame()
        count=0
        for path in song_files_df['file_path']:
            data_df = pd.read_json(path, lines=True)
            combined_song_data_df = combined_song_data_df.append(data_df)
            count+=1
            print(f"\t\t --> Processing {count} of {len(song_files_df)}", end ="\r")

        print(f"\t\t --> Got {len(combined_song_data_df)} song_data records")

        print(f"\t --> Saving all the song data to the DB")
        song_data_df = combined_song_data_df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()
        for i, row in song_data_df.iterrows():
            cur.execute(song_table_insert, list(row))
            conn.commit()
        print(f"\t\t --> Saved {len(song_data_df)} song_data records")

        print(f"\t --> Saving all the artists data to the DB")
        artist_data_df = combined_song_data_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()
        for i, row in artist_data_df.iterrows():
            cur.execute(artist_table_insert, list(row))
            conn.commit()
        print(f"\t\t --> Saved {len(artist_data_df)} song_data records")      
    except:
        print(f"There was an error running process_song_data: {traceback.format_exec()}")        
    
    
def process_log_data(cur, conn, file_path):  
    '''
    Processes all the song data and extracts the song and artist data and saves it in the DB

            Parameters:
                    a cur (psycopg2.extensions.cursor): A cursor object
                    b conn (psycopg2.extensions.connection): A connection object

            Returns:
                    None
    '''
    
    try:

        print("Starting to process log data")    
        log_files = get_files('data/log_data')
        log_files_df =  pd.DataFrame(log_files, columns=['file_path'])

        print(f"\t --> Got {len(log_files_df)} song_data files")

        print("\t --> Starting to load log data")
        combined_log_data_df = pd.DataFrame()
        count=0
        for path in log_files_df['file_path']:
            data_df = pd.read_json(path, lines=True)
            combined_log_data_df = combined_log_data_df.append(data_df)
            count+=1
            print(f"Processing {count} of {len(log_files_df)}", end ="\r")
        print(f"\t\t --> Got {len(combined_log_data_df)} log_data records")

        #Changing dtypes
        combined_log_data_df['ts'] = pd.to_datetime(combined_log_data_df['ts'])
        combined_log_data_df.loc[combined_log_data_df['userId']=='','userId'] = None

        print("\t --> Starting to process time_table")

        #filtering by "NextSong"
        df = combined_log_data_df[combined_log_data_df['page']=='NextSong'].copy()

        #converting the ts to datetime
        df['ts'] = pd.to_datetime(df['ts'], unit ='ms')

        #creating the time table
        df[['ts']], 
        t = df[['ts']].copy()
        t['hour'] = df['ts'].dt.hour
        t['day'] = df['ts'].dt.day
        t['week_of_year'] = df['ts'].dt.week
        t['month'] = df['ts'].dt.month
        t['year'] = df['ts'].dt.year
        t['weekday'] = df['ts'].dt.weekday
        time_df = t.rename(columns={'ts' : 'start_time'}).copy()

        #inserting records into the time_table
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
            conn.commit()

        print(f"\t\t --> Inserted {len(time_df)} records into the time_table")

        print("\t --> Starting to process user_table")

        user_df = df[['userId', 'firstName', 'lastName','gender','level']].drop_duplicates()
        user_df['userId'] = pd.to_numeric(user_df['userId'])
        user_df.sort_values(by=['userId'], inplace = True)
        user_df.reset_index(drop= True, inplace = True)

        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
            conn.commit()

        print(f"\t\t --> Inserted {len(user_df)} records into the users table")

        print("\t --> Starting to process songplay data")    
        count=0
        for index, row in combined_log_data_df.iterrows():

            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                song_id, song_title, song_duration, artist_id, artist_name = results

                # insert songplay record
                try:
                    start_time = row['ts']
                    user_id = row['userId']
                    level = row['level']
                    session_id = row['sessionId']
                    location = row['location']
                    user_agent = row['userAgent']    
                    songplay_data = [start_time, user_id, level, song_id, artist_id, session_id, location, user_agent]
                    cur.execute(songplay_table_insert, songplay_data)
                    conn.commit()
                    count+=1
                except:
                    print(f"{songplay_table_insert}")
                    print(f"{songplay_data}")
                    print(f"There was an error {traceback.format_exc()}")

        print(f"\t --> Added {count} entries to the songplays table")

    except:
        print(f"There was an error running process_log_data: {traceback.format_exc()}")            


def main():
    
    print("Starting the etl process")
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_song_data(cur, conn, 'data/song_data')
    process_log_data(cur, conn, 'data/log_data')
    
    cur.close()
    conn.close()
    print("Completed the etl process")


if __name__ == "__main__":
    main()