import sqlite3
import pandas as pd

DATA_PATH = "../../movies/"

db = sqlite3.connect("./database/Project_BDA.db")

def insert_dataframe(df : pd.DataFrame, 
                    table_name: str, columns: str or list, 
                    con: sqlite3.Connection):

    if type(columns) is list: columns = ','.join(columns)
    question_marks = ','.join(['?']*len(columns.split(',')))

    query = f"INSERT INTO {table_name} ({columns}) values({question_marks})"
    data = df.itertuples(index=False, name=None)
    try:
        cur = con.cursor()
        cur.executemany(query, data)
        cur.close()
        con.commit()
        print("Operation successfull!")
    except Exception as e:
        con.rollback()
        print("Error ocurred")
        print(e)

### Insert Movies Table ###
#movies = pd.read_csv(DATA_PATH + "movies.csv", header=0).merge(
#            pd.read_csv(DATA_PATH + "links.csv", header=0), 
#        on="movieId"
#    )
#movies.to_sql("movies", db, index=False, if_exists="append")

### Insert Tags Table ###
#tags = pd.read_csv(DATA_PATH + "tags.csv", header = 0)
#print(tags[(tags.userId==147) & (tags.movieId==4954) & (tags.timestamp==1173229521) ])
#insert_dataframe(tags, "tags", ["userid", "movieid", "tag", "ts"], db)


### Create Ratings Table ###
#ratings = pd.read_csv(DATA_PATH+ "ratings.csv", header=0)
#ratings.columns = ratings.columns.map(lambda x: x if x!="timestamp" else "ts")
#ratings.to_sql("ratings", db, if_exists="append", index=False)
#insert_dataframe(ratings, "ratings", ["userid", "movieid", "rating", "ts"], db)

### Create Genome_Scores Table ###
#genome_scores = pd.read_csv(DATA_PATH + "genome-scores.csv", header = 0)

### Create Genome_Tags Table ###
#genome_tags = pd.read_csv(DATA_PATH + "genome-tags.csv", header = 0)