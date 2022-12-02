"""Questions"""
from pprint import pprint

from pymongo import MongoClient
from utils import get_my_password, get_my_username

client = MongoClient(
    host="127.0.0.1",
    port=27017,
    username=get_my_username(),
    password=get_my_password(),
    authSource="admin",
)

db = client["paris"]
col = db["trees"]

pprint(col.find_one())


"""(a) Créer une requête pour retrouver les différentes valeurs de 'libellefrancais'"""

results = list(
    col.find(
        {"libellefrancais": {"$ne": " "}},
        projection={"libellefrancais": 1, "_id": 0},
        limit=10,
    )
)

pprint((results))

# or (None)
# results = list(
#     col.find(
#         {"libellefrancais": {"$ne": None}},
#         projection={"libellefrancais": 1, "_id": 0},
#         limit=10,
#     )
# )

# pprint((results))

"""(b) Créer une requête pour retrouver les différentes valuers de 'arrondissement'"""
results = list(
    col.find(
        {"arrondissement": {"$ne": " "}},
        projection={"arrondissement": 1, "_id": 0},
        limit=10,
    )
)

pprint((results))

"""(c) Créer une requête pour retrouver le nombre d'arbre qui ont une circonférence supérieure à 100 cm"""
results = list(
    col.find(
        {"circonferenceencm": {"$gt": 100.0}},
        projection={"circonferenceencm": 1, "_id": 0},
    )
)

print(len(results))
"""(d) Créer une requête pour retrouver le nombre d'arbre qui ont une circonférence supérieure à 100 cm et plus de 10 m 
de hauteur
"""
results = list(
    col.find({"circonferenceencm": {"$gt": 100.0}, "hauteurenm": {"$gt": 10.0}})
)

print(len(results))
"""(e) Créer une requête pour retrouver le nombre d'arbre qui ont une circonférence supérieure à 100 cm ou plus 
de 10 m de hauteur
"""
results = list(
    col.find(
        {"$or": [{"circonferenceencm": {"$gt": 100.0}}, {"hauteurenm": {"$gt": 10.0}}]}
    )
)

print(len(results))

"""(f) Créer une requête connaître le nombre de 'Platane' remarquables
"""
col.count_documents({"libellefrancais": "Platane"})

"""(g) Créer une requête pour rerouver les différents 'libellefrancais' qu'on peut trouver dans l''arrondissement' '
BOIS DE BOULOGNE'
"""
results = list(
    col.find(
        {
            "$and": [
                {"libellefrancais": {"$ne": " "}},
                {"arrondissement": {"$in": ["BOIS DE BOULOGNE"]}},
            ]
        },
        projection={"libellefrancais": 1, "_id": 0},
    )
)

pprint((results))

"""(h) Créer une requête pour retrouver les arbres qui possèdent une variété remplie 'varieteoucultivar'.
"""
results = list(
    col.find({"varieteoucultivar": {"$ne": " "}}).distinct("varieteoucultivar")
)


pprint((results))
"""(i) Créer une requête pour retrouver le nombre d'arbre dont le nom de l'espèce se termine en um par arrondissement
"""
import re

regex = re.compile(r"um$")


results = col.find(
    {"espece": regex},
    projection={"espece": 1, "_id": 0},
    sort=[("cp_arrondissement", -1)],
)
pprint(list(results))
"""(j) Créer une requête pour calculer le positionnement géographique moyen de toutes les espèces d'arbre. 
(on pourra aller consulter cette page pour cette question'
(https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/))"""
pipeline = [
    {
        "$group": {
            "_id": "$espece",
            " first": {"$avg": {"$arrayElemAt": ["$geo_point_2d", 0]}},
            "last": {"$avg": {"$arrayElemAt": ["$geo_point_2d", 1]}},
        }
    },
]

results = col.aggregate(pipeline)
pprint(list(results)[:10])
