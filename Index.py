import psycopg2
import pandas as pd

def create_database():
    #connect to the default database
    # conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=sunny")
    # conn.set_session(autocommit=True)
    # cur=conn.cursor()

    # #create sparkify database with UTF8 encoding
    # cur.execute("DROP DATABASE IF EXISTS artists")
    # cur.execute("CREATE DATABASE artists")

    # #close connection to default database
    # conn.close()

    #connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=artists user=postgres password=sunny")
    cur = conn.cursor()

    return cur, conn


#artists data read
# BestArtists = pd.read_csv("best_artists_RS.csv")
# BestArtists.head()
# Artists_clean = BestArtists[['Rolling Stone Magazine Rank', 'Artist', 'Main Grammy awards', 'Main Grammy Nominations', 'Total Awards']]
# Artists_clean.head()

#record data read
BestRecord = pd.read_csv("grammys-best record.csv")
BestRecord.head()

# #calling the function
cur, conn = create_database()


# #best artists query
# bestartists_table_create = ("""CREATE TABLE IF NOT EXISTS bestartists(
# rank numeric PRIMARY KEY,
# artists varchar,
# awards numeric,
# nominations numeric,
# total numeric
# )""")
# cur.execute(bestartists_table_create)
# conn.commit()


# #best record query
# bestrecords_table_create = ("""CREATE TABLE IF NOT EXISTS bestrecords(
# year numeric,
# record VARCHAR PRIMARY KEY,
# artist VARCHAR,
# genre VARCHAR
# )
# """)
# cur.execute(bestrecords_table_create)
# conn.commit()


# inserting data into artists table
# artists_table_insert = ("""INSERT INTO bestartists(
# rank,
# artists,
# awards,
# nominations,
# total)
# VALUES (%s, %s, %s, %s, %s)
# """)

# for i, row in Artists_clean.iterrows():
#     cur.execute(artists_table_insert, list(row))
# conn.commit()

#inserting data into record table
record_table_insert = ("""INSERT INTO bestrecords(
year,
record,
artist,
genre
) VALUES (%s, %s, %s, %s)
""")

for i, row in BestRecord.iterrows():
    cur.execute(record_table_insert, list(row))
conn.commit()


