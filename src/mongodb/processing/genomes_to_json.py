import pandas as pd
import json
import time

DATA_PATH = "../../../movies/"
movies = pd.read_csv(DATA_PATH + "movies.csv", header=0)
movie_ids = movies["movieId"] #for cycles

### Map tagIds of genomes to the actual values ###

#read csv to dataframe
genome_tags = pd.read_csv(DATA_PATH + "genome-tags.csv", header=0, index_col="tagId")
#The dataframe as only one column, since the index was set as the tagId values
#Transform the dataframe in a dictionary in format { tagId : tag }
genome_tags = dict(zip(genome_tags.index, genome_tags.tag))

genome_scores = pd.read_csv(DATA_PATH + "genome-scores.csv", header=0, index_col="movieId")
#Transform tagIds to respective tag, using genome_tags
genome_scores["tag"] = genome_scores.tagId.map(genome_tags)
genome_scores.drop(columns="tagId", inplace=True)
del genome_tags

genome_json = []
print("Starting to Iterate 'Genomes'")
tic = time.time()
#Iterate over every movie, and get 
for i, movie_id in enumerate(movie_ids):
    try:
        current_movie_genomes = genome_scores.loc[movie_id]
        genome_json.append(current_movie_genomes.to_dict(orient="records"))
    except KeyError:
        continue
toc = time.time()
del genome_scores
print("Genomes Processing Finished")
print(toc-tic)

with open("../data/genome_list.json","w") as f:
    json.dump(genome_json, f, indent=4)
    f.close()