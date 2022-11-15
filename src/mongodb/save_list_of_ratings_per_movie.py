import pandas as pd
import numpy as np
import json
import time

DATA_PATH = "../../movies/"
movies = pd.read_csv(DATA_PATH + "movies.csv", header=0)

### Read Ratings ###
ratings = pd.read_csv(DATA_PATH + "ratings.csv", index_col="movieId")
movie_ids = np.intersect1d(movies["movieId"], ratings.index.unique()) #for cycles
ratings_json = []

print("Starting to Iterate 'Ratings'")
tic = time.time()
for i,movie_id in enumerate(movie_ids):
    if i%100==0: print(i)
    current_movie_ratings = ratings.loc[movie_id]
    if (isinstance(current_movie_ratings, pd.DataFrame)):
        ratings_json.append(current_movie_ratings.to_dict(orient="records"))
    else:
        ratings_json.append(current_movie_ratings.to_frame().T.to_dict(orient="records"))    
toc = time.time()
print("Ratings Processing Finished")
del ratings
print(toc - tic)


with open("data/ratings_list.json","w") as f:
    json.dump(ratings_json, f, indent=4)
    f.close()