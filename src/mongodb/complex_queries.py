from pymongo import MongoClient
from time import time

client = MongoClient()
db = client.Project_BDA

#Selecionar os filmes com maior 'rating'.
#Os filmes em consideração para a pesquisa 
#são que têm uma certa tag como a mais relevante
tag = "007 (series)"
query = [   
            {"$group" : {
                    "_id" : "$movieId",
                    "relevance" : {"$max" : "$relevance"},
                }
            },
            {"$lookup" : {
                "from" : "Genome_Scores",
                "let" : {"movieid":"$_id", "rel":"$relevance"},
                "pipeline":[{
                    "$match" : {"$expr" : {
                        "$and" : [
                            {"$eq" : ["$movieId", "$$movieid"]},
                            {"$eq" : ["$relevance", "$$rel"]}
                        ]
                    }}
                }],
                "as" : "records"
                }
            },
            {"$unwind" : "$records"},
            { "$match" : {"records.tag" : tag}},
            # { "$project" : {
            #     "_id" : 1,
            #     "tag" : "$records.tag"
            # }},
            # {"$match" : {"tag" : tag}},
            {"$project" : {
                "_id" : 1,
                # "tag" : 0
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
                "title" : "$movie.title",
                "rating" : 1
            }},
            {"$sort" : {"rating" : -1}}
            # {"$limit" : 5}
        ]

#Obter os filmes cujas tags por 
# utilizador mais frequentes
# coincidem com as de um
# certo filme escolhido à priori
movieid = 1;
subsubquery = [
    {"$project" : {
        "_id" : 0,
        "movieId" : 1,
        "tag" : {"$toLower" : {"$trim" : {"input": "$tag"}}},
    }},
    {"$group" : {
        "_id" : {
            "movieid" : "$movieId", 
            "tag" : "$tag"
        },
        "counts" : {"$sum" : 1}
    }},
]
subquery = [
    *subsubquery,
    {"$group" : {
        "_id" : "$_id.movieid",
        "counts" : {"$max" : "$counts"}
    }},
    {"$lookup" : {
        "from" : "Tags",
        "let" : {
            "movieid": "$_id", 
            "counts" : "$counts"
        },
        "pipeline" : [
            *subsubquery,
            {"$match" : {
                "$expr" : {
                    "$and" : [
                        {"$eq" : ["$_id.movieid", "$$movieid"]},
                        {"$eq" : ["$counts", "$$counts"]}
                    ]
                }
            }},
        ],
        "as" : "tags"
    }},
    {"$unwind":"$tags"},
    # {"$limit" : 10}
]
query2 = [
    *subquery,
    {"$match" : {"_id" : movieid}},
    {"$project" : {
        "_id" : "$_id",
        "tag" : "$tags._id.tag",
    }},
    # {"$lookup" : {
    #     "from" : "Tags",
    #     "let" : {
    #         "movieid" : "$_id",
    #         "tag" : "$tag"
    #     },
    #     "pipeline" : [
    #         *subquery,
    #         {"$match" : {"_id" : {"$not" : {"$eq" : "$$movieid"}}}},
    #         {"$project" : {
    #             "_id" : "$_id",
    #             "tag" : "$tags._id.tag",
    #         }},
    #     ],
    #     "as" :"matches"
    # }},
    # {"$unwind" : "$matches"},
    # {"$project" : {
    #     "_id1" : "$_id",
    #     "tag1" : "$tag",
    #     "_id2" : "$matches._id",
    #     "tag2" : "$matches._tag"
    # }},
    # {"$match" : {
    #     "$expr" : {
    #         "$ne" : ["$_id", 1]
    #         }
    #     }
    # },
    {"$limit" : 10}
]

query3 = [
    {"$match" : {"_id" : 1}},
    {"$lookup" : {
        "from" : "Ratings",
        "localField" : "_id",
        "foreignField" : "movieId",
        "as": "rating"
    }},
    {"$unwind" : "$rating"},
    {"$group" : {
        "_id" : "$_id",
        "mean" : {"$avg" : "$rating.rating"}
    }}
]

other_query3 = [
    {"$match" : {"_id" : 1}},
    {"$project" : {
        "_id" : 1,
        "ratings" : {"$avg" : "$ratings.rating"}
    }},
]

coll_gen = db["Genome_Scores"]
coll_ratings = db["Ratings"]
coll_movies = db["Movies"]
coll_tags = db["Tags"]

start = time()
# docs = coll_gen.aggregate(query)
# docs = coll_tags.aggregate(query2)
docs = coll_movies.aggregate(query3)
print(time() - start)

input()
for item in docs:
    print(item)