"""Document-oriented databases via MongoDB:"""


# **********************************
"""to add data to the cluster we open mongo compass on the desktop
we connect the server  (mongodb+srv://ali-z11:password@cluster0.afgo8oi.mongodb.net/?retryWrites=true&w=majority) 
and we add the data"""

"""to start using mongo we need to use:"""
from pymongo import MongoClient

client = MongoClient(
    host="localhost",
    port=27017,
    username="***",
    password="***",
    authSource="admin",
)


db = client.test
# print(db)
# Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True, authsource='admin'), 'test')


# client.list_database_names()
# print(client.list_database_names())
# *****************************************

# -----------------------------------------------------------------------------
"""NoSQL DBMSs are made up of a wide range of technologies, techniques and paradigms, but three main families 
can be distinguished.

- Document-oriented databases
- Column-oriented databases
- Graph databases"""


"""A very simple analogy is Python dictionaries: you can think of document-oriented databases as a list of nested dictionaries:"""
# [
#     {
#         'prenom': 'Charles',
#         'age': 27,
#         'taille': 175,
#         'enfants': [
#                 {
#                     'prenom': 'Daniel',
#                     'date_de_naissance': date(2019, 6, 24)
#                 }
#             ]
#     },
#     {
#         'prenom': 'Paul',
#         'age': 25,
#         'taille': 185,
#         'style': True
#     }
# ]


""" - As you can see, this lack of a fixed data schema comes in handy when dealing with samples that have different shapes.
But what makes the greatest strength of document-oriented DBMSs is also their greatest weakness: 
while it's true that you can put whatever you want in your dataset, you might not want to put objects that are too different . 
It will be very difficult to search for useful information if you start mixing dishcloths and napkins.

- You indeed need an implicit schema to feed your database, otherwise it will be very difficult to retrieve data. 
For example, if you are storing content retrieved from twitter, it is important that you use the same keys for each of 
the tweets. Moreover, you are not going to put the tweets and the users in the same list: 
if you want to query the tweets on their identifier, you will have to make your query too complex...

- Moreover, relational databases are well suited to bring together data from different tables. 
This is not the typical use case for a document-oriented database management system: it can be done, but it's not that simple.

- It must be remembered that an effort must be made somewhere: for a relational database, it is during the realization 
of the schema of the tables before the storage. For a document-oriented DBMS, if the schematization effort has not 
been made then it is during the use of the data that the effort must be made."""
# ------------------------------------------------------------------------------------------------------------------------
"""Documents are stored in json format in databases which are themselves divided into collections. 
To make a comparison with RDBMSs, collections can be seen as arrays: you must try to put objects of the same type 
in the same collection: for example tweets must be stored in one collection and twitter users in another .

MongoDB has several clients, and in particular a python library called **pymongo**.

**Connection to MongoDB**
To connect to MongoDB, we will use the MongoClient class."""

"""(a) import the MongoClient class from the pymongo library"""

# from pymongo import MongoClient

"""To connect to MongoDB, you must use a username and password. We have already created an account for you. 
To recover your password and username, you can use the get_my_password and get_my_username functions 
contained in the utils.py file"""

"""(b) Run this cell to instantiate the MongoClient class."""

# from utils import get_my_password, get_my_username

# client = MongoClient(
#     host="127.0.0.1",
#     port=27017,
#     username=get_my_username(),
#     password=get_my_password(),
#     authSource="admin",
# )
# print(get_my_password())   we put the result in password =
# print(get_my_username())   we put the result in username =

# ---------------------------------------------------------------------------------------------------
"""Access to data"""

"""With this client we can list all databases available in MongoDB. You can do this using the list_database_names 
method of your client object."""

"""(c) list databases"""
# client.list_database_names()

"""You are only entitled to read privileges on the intro and paris databases. 
In this part, we will only use intro. You can access it using three different techniques:

- using square brackets: client['intro']
- call an attribute: client.intro
- call the get_database method: `client.get_database('intro')"""

"""(d) Access the intro database using any of these methods. 
Assign the result to an object named intro_database and print its type."""

# intro_database = client['intro']
# print(type(intro_database))

# or
# intro_database = client.intro
# print(type(intro_database))

# or
# intro_database = client.get_database('intro')
# print(type(intro_database))
# <class 'pymongo.collection.Collection'>

# print(intro_database)
# output:
# Database(MongoClient(host=['127.0.0.1:27017'], document_class=dict, tz_aware=False, connect=True, authsource='admin'), 'intro')

# ---------------------------------------------------------------------------------------------------
"""You can list collections using the list_collection_names method on the database object. The methods for accessing 
a particular collection are quite similar to those used to access databases:

- using square brackets
- call an attribute
- call the get_collection method"""


"""
(e) Call the list_collection_names method of the intro_database object."""
# print(intro_database.list_collection_names())

# output:
# ['teachers', 'courses']

"""(f) Access the teachers collection using one of the above methods. Assign the result to a teacher_collection object 
and print its type."""

# teacher_collection = intro_database['teachers']
# print(type(teacher_collection))

# or
# teacher_collection = intro_database.teachers
# print(type(teacher_collection))

# or
# teacher_collection = intro_database.get_collection('teachers')
# print(type(teacher_collection))
# <class 'pymongo.collection.Collection'>

# print(teacher_collection)
# output:
# Collection(Database(MongoClient(host=['127.0.0.1:27017'], document_class=dict, tz_aware=False, connect=True, authsource='admin'), 'intro'), 'teachers')
# ------------------------------------------------------------------------------------------------------------------------
"""For now, we'll just use the find and find_one methods of the Collection class. The first method returns a 
cursor object containing all the elements that meet the query criterion (to transform a cursor-type object into 
a list object, simply call the list function on the cursor). The second method returns only one result, the first, 
of the query.
"""

"""(g) try both methods without passing them any arguments"""

# print(list(teacher_collection.find()))
# [{'_id': ObjectId('5f3275f6c22f704983b3f67c'), 'id': 'id1', 'name': 'Paul Déchorgnat', 'age': 25, 'skills': ['Maths', 'ML', 'English']},
# {'_id': ObjectId('5f3275f6c22f704983b3f67d'), 'id': 'id2', 'name': 'Adrian El Baz', 'profession': 'Data Scientist'}]

# print(teacher_collection.find_one())
# {'_id': ObjectId('5f3275f6c22f704983b3f67c'), 'id': 'id1', 'name': 'Paul Déchorgnat', 'age': 25, 'skills': ['Maths', 'ML', 'English']}

"""You can already see that the data is indeed in the form of a json or Python dictionary. Note that there is an _id key 
which is always present in documents. This key is a unique identifier for each document.
"""
# ----------------------------------------------------------------------------------------------------------------
"""For some dictionaries and nested dictionary lists, you can use the pprint function from the pprint library instead of print.
 This will give a cleaner, clearer and prettier result (pprint literally means pretty print).
"""

"""(h) Using pprint on the contents of the courses collection"""
from pprint import pprint

# courses_collection = intro_database['courses']
# pprint(list(courses_collection.find()))

# output:
# [{'_id': ObjectId('5f327696c22f704983b3f67e'),
#   'id_course': 1,
#   'name': 'mongodb',
#   'required_skills': ['python', 'database']},
#  {'_id': ObjectId('5f327696c22f704983b3f67f'),
#   'id_course': 2,
#   'mode': 'console',
#   'name': 'Hadoop',
#   'required_skills': ['bash', 'linux', 'python']},
#  {'_id': ObjectId('5f327696c22f704983b3f680'),
#   'id_course': 3,
#   'mode': 'console',
#   'name': 'Streaming',
#   'required_skills': ['hadoop', 'bash', 'linux', 'python']}]

# --------------------------------------------------------------------------------------------------------------------------------
"""Conclusion

This introduction is very short and aims to give you an overview of what MongoDB can do while introducing the concept of a document-oriented database.

To remember
- document-oriented databases are similar to dictionary lists
- in MongoDB, data is stored in databases themselves divided into collections
- we use MongoClient to access the data"""
# --------------------------------------------------------------------------------------------------------------------------------
