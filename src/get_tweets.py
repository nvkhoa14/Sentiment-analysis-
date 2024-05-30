import pandas as pd
from pymongo import MongoClient
import json

CONNECTION_STRING = "mongodb+srv://nvkhoa14:UITHKT@cluster0.irat42q.mongodb.net/"

def mongoimport(csv_path, db_name, coll_name, connection_string):
    """ Imports a csv file to a mongo colection
    return: count of the documants in the new collection
    """
    client = MongoClient(connection_string)
    coll = client[db_name][coll_name]
    
    data = pd.read_csv(csv_path)
    payload = json.loads(json.loads(json.dumps(data.to_json(orient='records'))))
    
    coll.drop()
    n = coll.insert_many(payload)
    
    client.close()
    return len(n.inserted_ids)

client = MongoClient(CONNECTION_STRING)
mongoimport('./train.csv', 'lab4', 'chatgpt_tweets', CONNECTION_STRING)