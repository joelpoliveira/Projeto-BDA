import sqlite3
from time import time

db = sqlite3.connect("./database/Project_BDA.db")

#Selecionar os filmes com maior 'rating'.
#Os filmes em consideração para a pesquisa 
#são que têm uma certa tag como a mais relevante
tagid = 2
query = """ SELECT Movies.title, AVG(rating) AS avg_rating
             FROM Ratings
             INNER JOIN MOVIES
             ON Movies.movieid=Ratings.movieid
             GROUP BY Ratings.movieid
                HAVING COUNT(Ratings.movieid)>=100
                    AND Movies.movieid in (
                        SELECT movieid
                        FROM (
                            SELECT gs.movieid, gs.tagid
                            FROM Genome_Scores gs
                            INNER JOIN (
                                SELECT movieid, MAX(relevance) relevance
                                FROM Genome_scores
                                GROUP BY movieid
                            ) gs_max_relevance
                            ON gs.movieid = gs_max_relevance.movieid
                                AND gs.relevance = gs_max_relevance.relevance
                                AND gs.tagid = %d
                        )
                    )
             ORDER BY avg_rating DESC
         """ % (tagid)

# Adicionar tabela com as tags mais relevantes de cada filme(*)
# adicionar indice na tabela ratings, na coluna "movieId"
# adicionar indice na tabela (*) sob o camp tagid
query_2 = """
SELECT m.title, matches.avg_rating
FROM movies m
INNER JOIN (SELECT r.movieid, avg(rating) avg_rating, COUNT(*) counts
            FROM Movie_genome_tags_max_relevance tbl, Ratings r
            WHERE tbl.tagid = %d
                AND tbl.movieid = r.movieid
            GROUP BY r.movieid
            HAVING counts>100
        ) matches
ON m.movieid = matches.movieid
ORDER BY avg_rating

""" % (tagid)

#Obter os filmes cujas tags por 
# utilizador mais frequentes
# coincidem com as de um
# certo filme escolhido à priori
## ** Procurar por filmes semelhantes ao selecionado **
movieid = 1; 
subsubquery = """SELECT m.movieid movieid, m.title title, 
                            TRIM(LOWER(t.tag)) tag, 
                            COUNT(TRIM(LOWER(t.tag))) counts
                    FROM Movies m, Tags t
                    WHERE m.movieid = t.movieid
                    GROUP BY m.movieid, TRIM(LOWER(t.tag))"""
subquery = """SELECT counts_table.movieid, counts_table.title, counts_table.tag
                FROM (
                    %s
                    ) counts_table
                INNER JOIN (
                    SELECT movieid, title, tag, MAX(counts) counts
                    FROM (
                        %s
                    ) 
                    GROUP BY movieid 
                ) counts_max
                ON counts_table.movieid = counts_max.movieid
                    AND counts_table.counts = counts_max.counts
""" % (subsubquery, subsubquery)
query2 = """
            SELECT tbl.movieid, tbl.title, t.movieid, t.title
            FROM (
                %s
            ) tbl
            LEFT JOIN (
                %s
            ) t
            ON tbl.tag = t.tag
                AND tbl.movieid!=t.movieid
            WHERE tbl.movieid=%d
            ORDER BY t.movieid
""" % (subquery, subquery, movieid)

movieid = 1
query3="""SELECT m.movieid, AVG(rating)
            FROM Movies m, Ratings r
            WHERE m.movieid = r.movieid
            GROUP BY m.movieid 
            HAVING m.movieid=%d
""" % (movieid)
other_query3 = """SELECT r.movieid, AVG(rating)
            FROM Movies m, Ratings r
            WHERE m.movieid = r.movieid
            GROUP BY r.movieid 
            HAVING r.movieid=%d
""" % (movieid)

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