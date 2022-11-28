from pymongo import MongoClient
from time import time
import json

client = MongoClient()
db = client.Project_BDA

coll_gen = db["Genome_Scores"]
coll_ratings = db["Ratings"]
coll_movies = db["Movies"]
coll_tags = db["Tags"]

# userid = 134; rating = 3.5;
# query = [
#     { "userId" : userid },
#     { "$set" : {"rating" : rating}}
# ]

# start = time()
# coll_ratings.update_many(*query)
# print(time() - start)

with open("./data/movie_1_ratings.json", "r") as f:
    movie_1_ratings = json.load(f)

coll_movies.update_one({"_id" : 1}, {"$set" : {"ratings" : movie_1_ratings}})