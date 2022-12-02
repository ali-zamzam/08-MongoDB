"""Data Modification"""
import collections

from pymongo import MongoClient
from utils import get_my_password, get_my_username

client = MongoClient(
    host="localhost",
    port=27017,
    username="****",
    password="*****",
    authSource="admin",
)

"""In this part, we will modify a database. We must therefore use one on which you have write rights. 
We have created such a base. You can find its name using the get_my_database function of the utils.py file.
"""

"""
(c) Import get_my_database and store the database access in an object called db
(d) Print the collections contained in the database using list_collection_names."""

from utils import get_my_database

db = client[get_my_database()]

print(db.list_collection_names())

"""We can see that no collection is displayed. We will also notice that during the first lesson, when we displayed the 
databases, it did not appear. In fact, as long as the database is empty, it doesn't really exist. 
So you actually have the rights to create it. You can use the create_collection method with the name argument to 
create a collection.
"""
"""(e) Create a collection called test and assign the result of this command to an object called col."""
col = db.create_collection(name="test")

"""to delete a collections"""
# db.(collection_name).drop()
db.test.drop()

# or
db.drop_collection('test')
# --------------------------------------------------------------------------------------------------------------
"""Insert data"""

"""To insert data into a collection, you can use insert_one or insert_many which are col methods. 
The first inserts one observation while the second inserts several.
"""
"""(a) Insert the dictionary {'example': 123} into the test collection using insert_one."""
col.insert_one({"example": 123})

"""Note that this operation returns an object of type results."""

"""(b) Retry the previous operation, storing the result in a result object.
(c) Display the inserted_id attribute of result which contains the _id of the inserted documents."""
result = col.insert_one({"example": 123})
print(result.inserted_id)
# output: 633de7e8c32a288912f78e41

"""(d) Insert multiple objects using data defined below and using insert_many. Assign the result to a result object 
and display the inserted_ids attribute of result."""
data = [
    {"name": "Ali ZAMZAM", "study": "Ecole Centrale"},
    {"name": "Felix Faure", "age": 58},
]

result = col.insert_many(data)

print(result.inserted_ids)
# output : [ObjectId('633de95e437f1068c770ac0f'), ObjectId('633de95e437f1068c770ac10')]

"""To continue, we will copy the data from the paris database and in particular from the works collection into a new 
collection in your database called paris_work.
"""
"""
(e) Create a paris database connection called db_paris."""
db_paris = client.paris
# or 
db_paris = client["paris"]

"""(f) Create a connection to the works collection called col_paris."""
col_paris = db_paris.works

# or
col_paris = db_paris["works"]

"""(g) Create a query in such a way that it does not keep the _id attribute"""
data = list(col_paris.find(projection={'_id': 0}))

("""g) Insert the first ten results of this query into a new db collection called paris_work"""

db['paris_work'].insert_many(data[:10])


"""(i) Store access to this new paris_work collection in the col object."""
col = db["paris_work"]

"""(j) Check that the documents are indeed present in the collection. (We can use find or count_documents)"""

from pprint import pprint

data = col.count_documents(filter={})
print(data)
# --------------------------------------------------------------------------------------------------------------------
"""Update data"""

"""To update data, you can use the update_one or update_many methods of the Collection class. 
First we need to build a query to specify what data we want to change and then we use the '\$set' keyword to 
implement those changes.

For example, if we want to modify all the observations for which the key k1 is worth v1 by giving the key k2 the value v2,
 we can do:"""

# collection.update_many({k1: v1}, {'$set': {k2: v2}})
"""
(a) Modify the observations for which impact_circulation is 'RESTREINTE' so that this value is now in lower case.
(b) Create a query to verify that the change worked."""

value = 'RESTREINTE'

# mise à jour des valeurs
col.update_many({'impact_circulation': value}, 
                {'$set': {'impact_circulation': value.lower()}})

# récupération des données censées avoir été changées
results = col.find(filter={'impact_circulation': value.lower()})

from pprint import pprint

pprint(list(results))

"""If we want to limit these modifications to the first observation encountered, we will use update_one"""

# ----------------------------------------------------------------------------------------------------------------
"""Replace data"""

# collection. replace_one({k1: v1}, dict2)

"""(a) Using find_one, retrieve the _id of one of the documents.
(b) Replace this document with the dictionary {'error': 'data not found'}.
(c) Create a query to find the edited document"""

object_id = col.find_one()['_id']

col.replace_one({'_id': object_id}, {'error': 'data not found'})

pprint(col.find_one({'_id': object_id}))

# ----------------------------------------------------------------------------------------------------------------
"""Delete data"""

"""To delete documents, you can use delete_one or delete_many. You can still use a filter to select the documents to delete. 
For example, to delete all documents for which k1 is v1, we can do:
"""
# collection.delete_many({k1:v1})


"""(a) Delete the document we just edited
(b) Create a query to try to find it"""

col.delete_one(filter={'_id': object_id})
print(col.find_one(filter={'_id': object_id})

"""You can also delete a database or a collection:
"""
database.drop_collection('collection_name')
mongo_db_client.drop_database('database_name')


"""(c) Delete the 'paris_work' collection.
(d) List the collections available in your database."""

db.drop_collection('paris_work')
print(db.list_collection_names())
