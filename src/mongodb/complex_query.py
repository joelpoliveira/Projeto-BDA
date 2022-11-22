from pymongo import MongoClient
from time import time

client = MongoClient()
db = client.Project_BDA

query = [   { "$sort" : {"relevance": -1}},
            {"$group" : {
                    "_id" : "$movieId",
                    "relevance" : {"$max" : "$relevance"},
                    "tag" : { "$first" : "$tag" }
                }
            },
            { "$match" : {"tag" : "007"}},
            { "$project" : {
                "_id" : 1,
                "relevance" : 0,
                "tag" : 0
            }},
            {"$lookup" : {
                "from" : "Ratings",
                "localField" : "_id",
                "foreignField" : "movieId",
                "as" : "rating"
            }},
            {"$unwind" : "$rating"},
            {"$group" : {
                "_id" : "$_id",
                "rating" : {"$avg" : "$rating.rating"},
                "counts" : {"$sum" : 1}
            }}, 
            {"$match" : {
                "counts" : {"$gte" : 100}
            }},
            {"$lookup" : {
                "from" : "Movies",
                "localField" : "_id",
                "foreignField" : "_id",
                "as" : "movie"
            }},
            {"$unwind" : "$movie"},
            {"$project" : {
                "_id" : 0,
                "movie.title" : 1,
                "rating" : 1
            }}
        ]


coll_gen = db["Genome_Scores"]
coll_ratings = db["Ratings"]
coll_movies = db["Movies"]

start = time()
docs = coll_gen.aggregate(query)
print(time() - start)

input()
for item in docs:
    print(item)