from pymongo import MongoClient

client = MongoClient(host='172.17.0.2', port=27017)
print(client.list_database_names())
db = client["new_db"]
collection = db["new_collection"]
print( db.list_collection_names())