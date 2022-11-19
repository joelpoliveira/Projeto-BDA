from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid
import json
import os

client = MongoClient()
db = client.Project_BDA

def get_collection(name: str) -> Collection:
    try:
        collection = db.create_collection(name)
    except CollectionInvalid:
        collection = db[name]
    return collection   

# with open("../../schemas/MongoSchema.json", "r") as f:
#     schemas = json.load(f)["schemas"]

coll_movies=get_collection("Movies")
coll_genomes = get_collection("Genome Scores")
coll_tags = get_collection("Tags")
coll_ratings = get_collection("Ratings")
    
files = os.listdir("data")
files = list(
            filter(lambda x: not x.endswith("zip") and 
                             not x.startswith("all"), files))
key = list(
        map(lambda x: int(x[-1][:-1]),
            map(lambda file: file[:-4].split("_"), 
                files)
        )
    )
key = dict(zip(files, key))

## If only have partitions then -->
## Must unzip first
for file in sorted(files, key=lambda x: key[x]):
    if (not file.endswith(".zip") and not file.startswith("all")):
        with open(f"data/{file}") as f:
            try:
                print(f"Inserting {file}")
                data = json.load(f)
                for movie in data:
                    temp = movie["genome_scores"]
                    movie["genome_scores"] = coll_genomes.insert_many(temp).inserted_ids
                    
                    temp = movie["tags"]
                    movie["tags"] = coll_tags.insert_many(temp).inserted_ids

                    temp = movie["ratings"]
                    movie["ratings"] = coll_ratings.insert_many(temp).inserted_ids

                    movie["_id"] = movie.pop("movieId")
                    coll_movies.insert_one(movie)
                f.close()
            except Exception as e:
                print("Error Ocurred")
                print(e)
