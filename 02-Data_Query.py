"""for indexes and geograaphique point"""
# (https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/))
"""$arrayElemAt (aggregation)
Definition
$arrayElemAt
Returns the element at the specified array index."""
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


pipeline = [    
    {
        "$project": {
            "_id": "$espece", 
            " first": {"$avg": {"$arrayElemAt":["$geo_point_2d",0]}},
            "last": {"$avg": {"$arrayElemAt":["$geo_point_2d",1]}},
        }
    }
]

results = col.aggregate(pipeline)
pprint(list(results)[:10])

# ----------------------------------------------------------------------------
"""AND in MongoDB"""

# for advanced filters
"""$and"""
# Syntax: { $and: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
"""performs a logical AND operation on an array of one or more expressions (<expression1>, <expression2>, and so on) 
and selects the documents that satisfy all the expressions."""

# results = list(col.find({"$and" : [{"libellefrancais" : {"$ne" : " "}} , {"arrondissement": {"$in" : ["BOIS DE BOULOGNE"]}}]}
# , projection={"libellefrancais" : 1 ,"_id" : 0 }))

# pprint((results))


# or basic filters
"""In the find() method, if you pass multiple keys by separating them by ',' then MongoDB treats it as AND condition. 
Following is the basic syntax of AND """
# Syntax
# >db.mycol.find({key1:value1, key2:value2}).pretty()

# results = list(col.find(
#                 {"title": {"$ne":None},
#                 "authors": {"$ne":"Damien Chablat"}}
            
        
# ))

# print(len(results))
# ---------------------------------------------------------------------------------------
"""$or"""
"""The $or operator performs a logical OR operation on an array of one or more <expressions> and selects the documents 
that satisfy at least one of the <expressions>. 

The $or has the following syntax:"""
# { $or: [ { <expression1> }, { <expression2> }, ... , { <expressionN> } ] }

# ---------------------------------------------------------------------------------------
"""$in
The $in operator selects the documents where the value of a field equals any value in the specified array. 
To specify an $in expression, use the following prototype:"""

# { field: { $in: [<value1>, <value2>, ... <valueN> ] } }

# ---------------------------------------------------------------------------------------
"""$ne"""
"""Definition. **($ne)** selects the documents where the value of the field is not equal to the specified value
This includes documents that do not contain the field. """

# Syntax: { field: { $ne: value } }
# ---------------------------------------------------------------------------------------
"""$nin
$nin selects the documents where:
the field value is not in the specified array or the field does not exist.

If the field holds an array, then the $nin operator selects the documents whose field holds an array with no element 
equal to a value in the specified array (for example, <value1>, <value2>, and so on).
"""
# Syntax: { field: { $nin: [ <value1>, <value2> ... <valueN> ] } }

# ---------------------------------------------------------------------------------------
"""$lt"""
"""$lt selects the documents where the value of the field is less than (i.e. <) the specified value."""
# Syntax: { field: { $lt: value } }

# ---------------------------------------------------------------------------------------
"""$exists"""

"""When <boolean> is true, $exists matches the documents that contain the field, including documents where 
the field value is null. If <boolean> is false, the query returns only the documents that do not contain the field.
"""
# Syntax: { field: { $exists: <boolean> } }

# ---------------------------------------------------------------------------------------
"""$gt"""
"""$gt selects those documents where the value of the field is greater than (i.e. >) the specified value.
"""
# Syntax: { field: { $gt: value } }
# ---------------------------------------------------------------------------------------
"""$group"""
"""The $group stage separates documents into groups according to a "group key". The output is one document for each unique group key.

A group key is often a field, or group of fields. The group key can also be the result of an expression. 
Use the _id field in the $group pipeline stage to set the group key."""

# ---------------------------------------------------------------------------------------
"""$avg"""
"""Returns the average value of the numeric values. 
$avg ignores non-numeric values."""

# ---------------------------------------------------------------------------------------
"""$sort"""
"""Sorts all input documents and returns them to the pipeline in sorted order.

The $sort stage has the following prototype form:"""

# { $sort: { <field1>: <sort order>, <field2>: <sort order> ... } }

# ---------------------------------------------------------------------------------------
"""we will use a dataset on works in the streets of Paris"""

from pymongo import MongoClient
from utils import get_my_password, get_my_username

client = MongoClient(
    host="localhost",
    port=27017,
    username="ali-z11",
    password="A99216916o-",
    authSource="admin",
)


"""(c) Assign access to the paris database in an object named db."""
db = client["paris"]

"""(d) Assign access to the works collection in an object named col."""
col = db["works"]


"""(e) Print the first element of works using the find_one method and the pprint function of pprint."""

from pprint import pprint

# pprint(list(col.find()))
pprint(col.find_one())


"""These dictionaries have many attributes: some relate to the classification of the construction site while others indicate 
its location or its dates. 
The schema is a bit complex and some fields do not appear in all documents."""

from collections import Counter

"""(f) Retrieve all the data in the work using find and list and store them in an object named data."""
data = list(col.find())

"""(g) Create an object called fields_count that contains the number of fields per document."""
fields_count = list(map(lambda x: len(x), data))

"""(h) Create an object called counter that contains the number of documents per count field. 
(We want to know what is the distribution of the number of attributes per document).
you can use the Counter class from the collections library)."""
counter = Counter(fields_count)

"""(i) Print counter content"""
pprint(counter)


"""This quick analysis shows us why a NoSQL database can be a good idea for this data and especially a document-oriented 
database: the data does not have the same attributes from one record to another.
You may have noticed the 'geo_shape' attribute. This gives us the form of street works. 
We will draw some of the shapes to verify that the length of these lists are also different."""
import matplotlib.pyplot as plt
import numpy as np

%matplotlib inline

# récupération des coordonnées
coordinates_1 = np.array(data[0]['geo_shape']['coordinates'][0])
coordinates_2 = np.array(data[2]['geo_shape']['coordinates'][0])

# création du graphique
fig, axes = plt.subplots(1, 2, figsize=(16, 8))

# affichage de la première forme
axes[0].fill(coordinates_1[:, 0], coordinates_1[:, 1], color='b')
axes[0].set_title('street work 1')
axes[0].axis('off')


# affichage de la seconde forme
axes[1].fill(coordinates_2[:, 0], coordinates_2[:, 1], color='r')
axes[1].axis('off')
axes[1].set_title('street work 2')
plt.show()

# ----------------------------------------------------------------------------------------------------------------
"""Querys"""

"""In MongoDB, queries are made with dictionaries. To query all records whose key k1 has a value of v1 
and key k2 has a value of v2, we can do the following:"""

# collection.find(filter={k1:v1,k2:v2})

"""Note that the find method returns a Cursor object so if you want to collect the actual results you can use the list function:
"""
# results = list(collection. find({k1: v1, k2: v2}))


"""For nested dictionaries, you can use a ( . )to indicate a nested attribute. For example, you can do:

This will give you the observations for which the key k1 is associated with a dictionary and, in this dictionary, 
the key sub_key1 is associated with the value v1."""
# results = list(collection.find({k1.sub_key1:v1}))

"""(a) Create a query to get all records from the works collection for which the status key is 4. 
Assign the results to an object named results and display its length"""
# results = list(col.find(filter={'statut': 4}))
# or
results = list(col.find({"statut" :4}))

print(len(results))

"""(b) Build a query to get all records in the works collection for which the status key is set to 3 and 
AND the disturbance_level key is set to 1. Assign the results to an object named results and display its length ."""

results = list(col.find({"statut" : 3 , "disturbance_level" :1}))
print(len(results))

"""(c) Build a query to get all records in the works collection for which the fields dictionary has a status key associated 
with the value 4 OR a disturbance_level key associated with the value 1. Assign the results to an object named 
results and display its length."""

results = list(col.find({"$or" : [{"statut ": 4}, {"disturbance_level" :1}]}))
print(len(results))


"""(d) Create a query to obtain all the records of the works collection for which the maitre_ouvrage key is different 
from the value 'City of Paris'. Assign the results to an object named results and display its length."""

results = list(col.find({"maitre_ouvrage" : {"$ne" : "city of paris"}}))
print(len(results))

"""
Instead of checking values one by one, we can also see whether or not they are in a value selection using '\$in' or '\$nin':
"""
# field k1 is in [1, 2, 3, 4]
results = collection.find(filter={k1:{"$in": [1, 2, 3, 4]}})
# field k1 is not in [5, 6, 7]
results = collection.find(filter={k1: "$nin": [5, 6, 7]}})


"""(e) Create a query to get all records in the works collection for which the cp_arrondissement key is included in 
the values ['75018', '75017', '75014', '75005']. Assign the results to an object named results and display its length."""

results = list(col.find({"cp_arrondissement" : {"$in" : ['75018', '75017', '75014', '75005']}}))
print(len(list(results)))


"""(f) Create a query to get all records in the works collection for which the key stv_num is less than 10. 
Assign the results to an object named results and display its length."""

results = list(col.find({"numero_stv" : {"$lt" : 10} }))
print(len(results))


"""We can also check for the existence of an attribute using the value '\$exists'. For example, if we want to verify that 
the key k2 is present, we can do the following:
"""
# results = collection.find(filter={k2:{'$exists': True}})


"""(g) Build a query to get all records in the works collection for which the url_lic key exists. 
Assign results to an object named resuls and display its length"""

results = list(col.find({"url_lic": {"$exists": True}}))
print(len(results))


"""Using Python allows us to use regular expressions as well. If we want to find all records whose value associated 
with key k1 with a given regex regular expression, we can use 're':"""
# regex = re.compile('a given regular expression')
# results = collection.find(filter={'k1': regex})

"""(h) Import the 're' package."""
import re

"""(i) Compile a regular expression matching a string of characters starting with the word 'city' or 'City' called regex"""
regex = re.compile("^[Cc]ity")

"""(j) Create a query to get all the records in the works collection for which the master_work key matches this 
regular expression. Assign results to an object named results and display its length"""
results = list(col.find({"maitre_ouvrage" : regex}))
print(len(results))

# ----------------------------------------------------------------------------------------------------------------
"""Projection of results"""

"""We've seen how to make complicated queries, but each time we take all the data. If we want to limit the results to 
certain attributes, we can use the second argument of the find method, i.e. projection. 
This argument takes a dictionary of the various fields and a 1 if they should be included in the results or a 0 if 
they should be excluded. By default, the '_id' attribute is always included but it can be excluded by specifying a 0.

(that's mean without using the prokection we will have all results from the database that's related to the filter)"""
# [{'_id': ObjectId('5f327503c22f704983b3f13e'),
#   'cp_arrondissement': '75013',
#   'date_creation': '2019-02-13',
#   'date_debut': '2019-03-11',
#   'date_fin': '2019-05-17',
#   'date_maj': '2019-02-27',
#   'description': 'Fuite sur réseau.',
#   'geo_point_2d': [48.83076245747994, 2.3564704425954357],
#   'geo_shape': {'coordinates': [[[2.356545744184811, 48.83081866798808],
#                                  [2.3564939511474963, 48.83071118423303],
#                                  [2.356470560562706, 48.8307005357294],
#                                  [2.356427324221482, 48.83069930387801],
#                                  [2.356404140565263, 48.83073324552232],
#                                  [2.356403111128202, 48.830755279084215],
#                                  [2.356432944762973, 48.83079752121979],
#                                  [2.356534946120683, 48.83083264008953],
#                                  [2.356545744184811, 48.83081866798808]]],
#                 'type': 'Polygon'},
#   'identifiant': 'CP001106',
#   'impact_circulation': 'RESTREINTE',
#   'impact_circulation_detail': "Neutralisation d'une file de circulation.",

#   'maitre_ouvrage': 'CPCU',

"""(but after using the projection we will have)"""
# 'maitre_ouvrage': 'CPCU'},
#  {'maitre_ouvrage': 'RATP'},
#  {'maitre_ouvrage': 'STV'},
#  {'maitre_ouvrage': 'CPCU'},
#  {'maitre_ouvrage': 'Climespace'},
#  {'maitre_ouvrage': 'SAGP'},
#  {'maitre_ouvrage': 'CPCU'},
#  {'maitre_ouvrage': 'SAGP'},
#  {'maitre_ouvrage': 'Eau de Paris'},
#  {'maitre_ouvrage': 'STV'},
#  {'maitre_ouvrage': 'SAGP'},
#  {'maitre_ouvrage': 'RATP'},

"""For example, if we only want values associated with key k2 for records where key k1 is associated with value v1, 
we can use the following:
"""
# results = collection.find(filter={k1:v1}, projection={k2:1, '_id':0})


"""(a) Create a query to get all the records of the works collection for which the maitre_ouvrage key is different from 
the value 'City of Paris' but including nothing other than this key. Assign the results to an object named results and 
display it."""

# results = list(col.find({"maitre_ouvrage" :{"$ne" : "Ville de Paris"}}, {"maitre_ouvrage" : 1 ,"_id" : 0 }))

results = list(col.find({"maitre_ouvrage" :{"$ne" : "Ville de Paris"}}, projection={"maitre_ouvrage" : 1 ,"_id" : 0 }))

pprint(results)


"""If we want the distinct values of a field, we can use a different method of the Collection class: distinct. 
To combine them with a query, we can chain them. If we take the example from the last point, we can only get the
distinct values by doing:
"""
# results = collection.find(filter={k1:v1}.distinct(key=k2)


"""(that's mean instaed of had this list """
# 'maitre_ouvrage': 'CPCU'},
#  {'maitre_ouvrage': 'RATP'},
#  {'maitre_ouvrage': 'STV'},
#  {'maitre_ouvrage': 'CPCU'},
#  {'maitre_ouvrage': 'Climespace'},
#  {'maitre_ouvrage': 'SAGP'},]

"""we can had this one"""
# ['CPCU',
# 'RATP',
# 'STV',
# 'Allianz',
#  'Altarea -Cogedim',
#  "Amb d'Arabie Saoudite",
#  'Ambassade Côte d¿Ivoire',]


results = list(col.find({"maitre_ouvrage" : {"$ne" : "Ville de Paris"}}).distinct("maitre_ouvrage"))
pprint(results)


"""sort"""
"""we can sort the results according to one or more keys using the sort argument. 
For example, to sort the documents according to the key k1, we can do:"""

# collection.find(sort=[('k1', 1)]

"""The sort argument must be a list of tuples of size 2. For each tuple, the first value is the key to use for sorting 
and the second must be 1 to sort in ascending order by this key or -1 for descending order."""

"""(c) Create a query to get only the cp_roundings in descending order. 
Assign the results to an object call results and print it."""


results = col.find(sort=[('cp_arrondissement', -1)],
                   projection={'cp_arrondissement': 1, '_id': 0})
pprint(list(results))

"""You can of course combine sort, filter and projection to obtain the results you want. 
Note also that you can call the limit method on a cursor type object to limit the number of results."""

results = col.find( {'maitre_ouvrage':{'$ne': 'Ville de Paris'}},
                   projection={'maitre_ouvrage': 1, '_id': 0}, sort=[('cp_arrondissement', -1)], limit=10)
pprint(list(results))
# [{'maitre_ouvrage': 'STV'},
#  {'maitre_ouvrage': 'STV'},
#  {'maitre_ouvrage': 'RATP HTA'},
#  {'maitre_ouvrage': 'DVD STV'},
#  {'maitre_ouvrage': 'CPCU'},
#  {'maitre_ouvrage': 'RTE'},
#  {'maitre_ouvrage': 'STV'},
#  {'maitre_ouvrage': 'RATP HTA'},
#  {'maitre_ouvrage': 'RATP'},
#  {'maitre_ouvrage': 'RATP'}]
# ------------------------------------------------------------------------------------------------------
""" Aggregation"""

"""We may also want to aggregate our data. This can be very useful to perform some checks on our data.
First, we can simply count the number of records that meet a given criterion: for example, if we want to count the 
records for which k1 is associated with the value v1:"""

# collection.count_documents(filter={k1:v1})

"""(a) Create a query to get the number of records for which maitre_ouvrage is 'RATP' using the count_documents function"""

print(col.count_documents({"maitre_ouvrage": "RATP"}))

"""To make more complex queries, you can use the aggregate method. We will have to use pipelines: 
these are lists of actions to apply to a collection. To group the data, we use the keyword $group and we specify
 the aggregation key with _id, the name of the fields to aggregate and the action to do.

For example, if we want to sum the values associated with the key k2 for each of the values of k1, 
we can define the following pipeline:"""
# pipeline = [
#      {"$group": 
#       {"_id": "$k1",
#        "sum_k2": {"$sum": "$k2"}
#       }
#      }
#  ]

# Once this pipeline is defined, we can use the aggregate method:

# collection.aggregate(pipeline=pipeline)


"""(b) Create a query to obtain the average ($avg) of niveau_perturbation by cp_arrondissement. 
Assign the results to a results object and print it"""

pipeline = [
    {'$group': {'_id': '$cp_arrondissement',
                'mean_perturbation': {'$avg': '$niveau_perturbation'}
               }
    }
]

results = col.aggregate(pipeline)
pprint(list(results))


"""In a pipeline, we can of course use filters. They are defined in the same way as previously but they are introduced by a 
keyword \$match. Thus, if we take the previous example, but only want to aggregate the results for 
which k3 equals v3, we can do:"""


# pipeline = [
#      {"$match": {"k3": "v3"}},
#      {"$group":
#       {"_id": "$k1",
#        "sum_k2": {"$sum": "$k2"}
#       }
#      }
# ]

"""(c) Rewrite the previous pipeline so as not to take into account the cp_arrondissement which is None"""

pipeline = [ {'$match': 
       {'cp_arrondissement': {'$ne': None}}
      },
     {'$group': {'_id': '$cp_arrondissement', 'mean_perturbation': {'$avg': '$niveau_perturbation'}}}]

list(col.aggregate(pipeline))
# [{'_id': '75017', 'mean_perturbation': 1.7547169811320755},
#  {'_id': '75013', 'mean_perturbation': 1.7565217391304349},
#  {'_id': '75020', 'mean_perturbation': 1.8},
#  {'_id': '75007', 'mean_perturbation': 1.3529411764705883},
#  {'_id': '75019', 'mean_perturbation': 1.654320987654321},
#  {'_id': '75009', 'mean_perturbation': 1.8541666666666667},
#  {'_id': '75001', 'mean_perturbation': 1.8153846153846154},
#  {'_id': '75012', 'mean_perturbation': 1.6666666666666667}]


pipeline = [ {'$match': 
       {'cp_arrondissement': {'$in': ["75015"]}}
      },
     {'$group': {'_id': '$cp_arrondissement', 'mean_perturbation': {'$avg': '$niveau_perturbation'}}}]

list(col.aggregate(pipeline))
# [{'_id': '75015', 'mean_perturbation': 1.6551724137931034}]


pipeline = [{'$match': 
       {'cp_arrondissement': {'$in': ["75015","75018","75019"]}}
      },
     {'$group': {'_id': '$cp_arrondissement', 
    'mean_perturbation': {'$avg': '$niveau_perturbation'}}},
      {"$sort": { "cp_arrondissement": 1 }}]

list(col.aggregate(pipeline))
# [{'_id': '75015', 'mean_perturbation': 1.6551724137931034},
#  {'_id': '75018', 'mean_perturbation': 1.8055555555555556},
#  {'_id': '75019', 'mean_perturbation': 1.654320987654321}]


pipeline = [{'$match': 
       {'cp_arrondissement': {'$in': ["75015","75018","75019"]}}
      },
     {'$group': {'_id': '$cp_arrondissement', 
    'mean_perturbation': {'$avg': '$niveau_perturbation'}}},
      {"$sort": { "cp_arrondissement": -1 }}]

# [{'_id': '75015', 'mean_perturbation': 1.6551724137931034},
#  {'_id': '75018', 'mean_perturbation': 1.8055555555555556},
#  {'_id': '75019', 'mean_perturbation': 1.654320987654321}]

pipeline = [{'$match': 
       {'cp_arrondissement': {'$ne': None}}
      },
     {'$group': {'_id': '$cp_arrondissement', 
    'mean_perturbation': {'$avg': '$niveau_perturbation'}}},
      {"$sort": { "mean_perturbation": -1 }}]

list(col.aggregate(pipeline))
# [{'_id': '75002', 'mean_perturbation': 1.934782608695652},
#  {'_id': '75008', 'mean_perturbation': 1.8918918918918919},
#  {'_id': '75016', 'mean_perturbation': 1.8688524590163935},
#  {'_id': '75009', 'mean_perturbation': 1.8541666666666667},
#  {'_id': '75010', 'mean_perturbation': 1.8266666666666667},
#  {'_id': '75001', 'mean_perturbation': 1.8153846153846154},
#  {'_id': '75018', 'mean_perturbation': 1.8055555555555556},
#  {'_id': '75020', 'mean_perturbation': 1.8}]

"""if we need to did a projection"""
# {"$project":
#          {"K1": 1, "_id": 0}}
