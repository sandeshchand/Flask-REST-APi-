
from flaskext.mysql import MySQL
import MySQLdb
import mysql.connector as connection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pymongo
import logging as lg

lg.basicConfig(filename="test.log",level = lg.INFO, format='%(asctime)s,%(message)s')
class config():
    def __init__(self,host,database,user,passwd ):

        self.host = host
        self.database =database
        self.user = user
        self.passwd = passwd



    def db_connect(self):
        try:
            #mydb = connection.connect(host="localhost",database="my_sql",user="root",passwd="********",use_pure=True)
            self.mydb = connection.connect(host=self.host, database=self.database, user=self.user, passwd=self.passwd,
                                      use_pure=True)
            lg.info('MySql_db is connected succesfully !!')
            return self.mydb
        except Exception as e:
            lg.error("error has been occured.")
            lg.exception(str(e))

class config_cassa():
    def __init__(self, id, key):
        self.id = id
        self.key = key

    def cassandra_connect(self):
        try:
            cloud_config = {
                'secure_connect_bundle': r'C:\Users\flaskProject\secure-connect-cassandra-db.zip'
            }
            '''
            auth_provider = PlainTextAuthProvider('jPsRZvewlXbuAwrPSdxFYROd',
                                                  '***************.j')
            '''
            auth_provider = PlainTextAuthProvider(self.id,self.key)

            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            lg.info('Cassandra_ session  is  connected  succesfully !!')
            return session
        except Exception as e:
            lg.error("error has been occured.")
            lg.exception(str(e))

class config_mongo():

    def __init__(self, db_name):
        self.db_name = db_name

    def mongodb_connect(self):

        try:
            DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/"
            #db_name = "Mongo_db1"
            client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
            # print(client)
            database = client[self.db_name]
            lg.info('Mongodb_db is  connected  succesfully !!')
            return database

        except Exception as e:
            lg.error("error has been occured.")
            lg.exception(str(e))


#config_obj= config("localhost","my_sql","root","Kanchanpur12$")
#config_obj.db_connect()
