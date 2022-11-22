import sqlite3

from time import time

db = sqlite3.connect("./database/Project_BDA.db")


#Selecionar as 10 Tags mais relevantes de um filme
query1 = """SELECT tag
            FROM Tags
            INNER JOIN (
                SELECT tagid
                FROM Genome_Scores
                WHERE movieid=1
                ORDER BY relevance DESC
                LIMIT 10
                ) Genomes
            ON Tags.tagid = Genomes.tagid
            """

#Selecionar os filmes com maior 'rating'
#cujo genome indentificou certa 'tag' 
#como a mais relevante
tagid = 1
query2 = """ SELECT Movies.title, AVG(rating) AS avg_rating
             FROM Ratings
             INNER JOIN MOVIES
             ON Movies.movieid=Ratings.movieid
             GROUP BY Ratings.movieid
                HAVING COUNT(Ratings.movieid)>=100
                    AND Movies.movieid in (
                        SELECT movieid
                        FROM (
                            SELECT movieid, MAX(relevance)
                            FROM Genome_scores
                            GROUP BY movieid
                                HAVING tagid = %d
                        )
                    )
             ORDER BY avg_rating DESC
             LIMIT 10
         """ % (tagid)

try:
    cur = db.cursor()
    start = time()
    cur.execute(query2)
    print(time() - start)

    input()
    result = cur.fetchall()
    for item in result:
        print(item)
except Exception as e:
    print(e)