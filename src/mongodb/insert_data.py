from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
import json
import os

client = MongoClient()
db = client.Project_BDA
try:
    movies = db.create_collection("Movies")
except CollectionInvalid:
    movies = db["Movies"]
    
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
                movies.insert_many(data)
                f.close()
            except Exception as e:
                print("Error Ocurred")
                print(e)
