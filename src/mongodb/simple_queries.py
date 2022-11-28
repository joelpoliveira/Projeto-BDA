from pymongo import MongoClient
from time import time
from datetime import datetime

client = MongoClient()
db = client.Project_BDA

coll_gen = db["Genome_Scores"]
coll_ratings = db["Ratings"]
coll_movies = db["Movies"]
coll_tags = db["Tags"]

movieid = 3000
query = [{ "genres" : {"$regex" : ".*Action.*" }}, 
         {"title" : 1, "_id" : 0}]
query2 = [
    {   
        "$and" : [
                {"timestamp" : 
                    {"$gt" : datetime(1980,1,1).timestamp()}
                },
                {"timestamp" : 
                    {"$lt" : datetime(2022,1,1).timestamp()}
                }
            ]
    },
    {"userid":1, "_id":0, "tag":1}]

query3 = [{"_id" : 1}, {"ratings" : 1, "_id" : 0}]

start = time()
# results = coll_movies.find(*query)
results = coll_movies.find(*query3)
# results = coll_tags.find(*query2)
print(time() - start)

input()
for item in results:
    print(item)
