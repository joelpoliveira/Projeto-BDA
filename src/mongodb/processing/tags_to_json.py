import pandas as pd
import json
import time

DATA_PATH = "../../../movies/"
movies = pd.read_csv(DATA_PATH + "movies.csv", header=0)
movie_ids = movies["movieId"] #for cycles

### Read Movie Tags ###
movie_tags = pd.read_csv(DATA_PATH + "tags.csv", index_col="movieId")
tags_json = []

print("Starting to Iterate 'Tags'")
tic=time.time()
for i, movie_id in enumerate(movie_ids):
    try:
        current_movie_tags = movie_tags.loc[movie_id]
        tags_json.append(current_movie_tags.to_dict(orient="records"))
    except KeyError:
        continue
    except TypeError:
        tags_json.append(current_movie_tags.to_frame().T.to_dict(orient="records"))
toc = time.time()
del movie_tags

print("Tags Processing Finished")
print(toc-tic)

with open("../data/tags_list.json","w") as f:
    json.dump(tags_json, f, indent=4)
    f.close()