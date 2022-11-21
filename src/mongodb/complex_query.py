from pymongo import MongoClient

client = MongoClient()
db = client.Project_BDA

query2 = [  { "$sort" : {"relevance": -1}},
            {"$group" : {
                    "_id" : "$movie_id",
                    "relevance" : {"$max" : "$relevance"},
                    "tag" : { "$first" : "$tag" }
                }
            },
            { "$match" : {"tag" : "007"}},
            { "$sort" : {"_id" : 1}},
            { "$project" : {
                "_id" : 1,
                "relevance" : 0,
                "tag" : 0
            }},
        ]

coll_gen = db["Genome Scores"]

docs = coll_gen.aggregate(query2)

for item in docs:
    print(item)