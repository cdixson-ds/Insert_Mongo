import pymongo
import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd

load_dotenv()

DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
DB_NAME = os.getenv("DB_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{DB_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")

connection = sqlite3.connect(DB_FILEPATH)

cursor = connection.cursor()

query = "SELECT * FROM charactercreator_character;"
df = pd.read_sql_query(query, connection)

#print(df.head())

client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)

db = client.test_database_2 # "test_database" or whatever you want to call it
print("----------------")
print("DB:", type(db), db)

collection = db.rpg_collection 
print("----------------")
print("COLLECTION:", type(collection), collection)


db.rpg_collection.insert_many(df.to_dict('records'))


print("----------------")
print("COLLECTIONS:")
print(db.list_collection_names())
print("DOCS:", collection.count_documents({})) # select *

connection.close()






