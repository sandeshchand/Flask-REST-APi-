import mysql.connector as connection
import pymongo


try:
    DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
    print(client)
    db= client["Mongodb1"]
    print(client.list_database_names())

except Exception as e:
        print(e)

