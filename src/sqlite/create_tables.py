import sqlite3

db = sqlite3.connect("./database/Project_BDA.db")

movies_query = """CREATE TABLE movies (
                movieid INTEGER PRIMARY KEY,
                title	 VARCHAR(512) NOT NULL,
                genres	 VARCHAR(512) NOT NULL,
                imdbid	 INTEGER,
                tmdbid	 INTEGER
            )"""

tags_query = """CREATE TABLE tags (
                    userid	 INTEGER,
                    movieid	 INTEGER NOT NULL,
                    tag		 VARCHAR(512),
                    ts		 DATETIME,
                    PRIMARY KEY(userid,movieid,ts),
                    FOREIGN KEY (movieid) REFERENCES Movies(movieId)
                )"""

ratings_query = """CREATE TABLE ratings (
        userid	 INTEGER,
        movieid	 INTEGER,
        rating	 FLOAT(2),
        ts		 DATETIME,
        PRIMARY KEY(userid,movieid),
        FOREIGN KEY (movieid) REFERENCES Movies(movieId)
    )"""

genome_tags = """CREATE TABLE genome_tags (
        tagid INTEGER PRIMARY KEY,
        tag	 TEXT(512)
    )"""

genome_scores = """CREATE TABLE genome_scores (
	movieid		 INTEGER NOT NULL,
	tagid		 INTEGER NOT NULL,
	relevance	 FLOAT(8),
	PRIMARY KEY(movieid,tagid),
	FOREIGN KEY (movieid) REFERENCES Movies(movieId),
	FOREIGN KEY (tagid) REFERENCES Genome_Tags(tagId)
)"""


try:
    cur = db.cursor()
    cur.execute(movies_query)
    cur.execute(tags_query)
    cur.execute(ratings_query)
    cur.execute(genome_tags)
    cur.execute(genome_scores)
    
    db.commit()
    print("Operation succesfull")
except Exception as e:
    db.rollback()
    print("An error ocurred")
    print(e)
