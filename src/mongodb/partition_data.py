import json

with open("data/data_partition_1.json", "r") as f:
    data = json.load(f)
    f.close()

N = len(data)
partition_size = int(N/2)

i = 0
while (i * partition_size < N):
    print(i)
    start = i * partition_size
    end = (i+1) * partition_size
    data_partition = data[start:end]

    with open(f"data/data_partition_{i+1}_1.json", "w") as f:
        json.dump(data_partition, f)
        f.close()

    i+=1