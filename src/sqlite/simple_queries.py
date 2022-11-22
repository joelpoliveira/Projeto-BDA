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