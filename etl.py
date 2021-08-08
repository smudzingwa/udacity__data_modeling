import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# def process_song_file(cur, filepath):
#     # open song file
#     df = pd.read_json(filepath, lines=True)

#     # insert song record
#     song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()
#     cur.execute(song_table_insert, song_data)
    
#     # insert artist record
#     artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()
#     cur.execute(artist_table_insert, artist_data)


# def process_log_file(cur, filepath):
#     # open log file
#     df = 

#     # filter by NextSong action
#     df = 

#     # convert timestamp column to datetime
#     t = 
    
#     # insert time data records
#     time_data = 
#     column_labels = 
#     time_df = 

#     for i, row in time_df.iterrows():
#         cur.execute(time_table_insert, list(row))

#     # load user table
#     user_df = 

#     # insert user records
#     for i, row in user_df.iterrows():
#         cur.execute(user_table_insert, row)

#     # insert songplay records
#     for index, row in df.iterrows():
        
#         # get songid and artistid from song and artist tables
#         cur.execute(song_select, (row.song, row.artist, row.length))
#         results = cur.fetchone()
        
#         if results:
#             songid, artistid = results
#         else:
#             songid, artistid = None, None

#         # insert songplay record
#         songplay_data = 
#         cur.execute(songplay_table_insert, songplay_data)

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_data(cur, conn, file_path):
    """
    This processes all the song data extracts the song and artist data and saves it in the DB
    """
    
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
    
def process_log_data(cur, conn, file_path):  
    """
    This processes all the song data extracts the song and artist data and saves it in the DB
    """
    
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
    count = 0
    for index, row in combined_log_data_df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        try:
            songplay_id = index+1
            start_time = row['ts']
            user_id = row['userId']
            level = row['level']
            session_id = row['sessionId']
            location = row['location']
            user_agent = row['userAgent']    
            songplay_data = [songplay_id, start_time, user_id, level, songid, artistid, session_id, location, user_agent]
            cur.execute(songplay_table_insert, songplay_data)
            count+=1
            conn.commit()
        except:
            print(f"{songplay_data}")
            print(f"There was an error {traceback.format_exec()}")
    
    print(f"\t\t --> Inserted {count} records into the songplay_table")
        
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


        


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_song_data(cur, conn, 'data/song_data')
    process_log_data(cur, conn, 'data/log_data')
    
    

    #process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    #process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    print("Completed the etl process")


if __name__ == "__main__":
    main()