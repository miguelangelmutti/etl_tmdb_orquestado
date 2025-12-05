import json
list_movies_ids = []
with open('all_changes_results.json') as f:
    data = json.load(f)

for i in data:
    for j in i['results']:
        list_movies_ids.append(j['id'])
print("ids en  all_changes_results: " + str(len(list_movies_ids)))        

with open('changed_movies_results.json') as f:
     data = json.load(f)

print("ids en  changed_movies_results: " + str(len(data)))



