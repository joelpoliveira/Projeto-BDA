import sqlite3
from time import time

db = sqlite3.connect("./database/Project_BDA.db")

#Selecionar os filmes com maior 'rating'.
#Os filmes em consideração para a pesquisa 
#são que têm uma certa tag como a mais relevante
tagid = 1
query = """ SELECT Movies.title, AVG(rating) AS avg_rating
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
         """ % (tagid)

try:
    cur = db.cursor()
    start = time()
    cur.execute(query)
    print(time() - start)

    input()
    result = cur.fetchall()
    for item in result:
        print(item)
except Exception as e:
    print(e)