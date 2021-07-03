
#from config import db_connect, cassandra_connect,mongodb_connect,config_obj.db_connect
from config import config,config_cassa,config_mongo
from flask import Flask,render_template,request,jsonify
import mysql.connector as connection
import csv
import logging as lg
lg.basicConfig(filename="test.log",level = lg.INFO, format='%(asctime)s,%(message)s')

app= Flask(__name__)


@app.route('/create_table', methods=['POST'])
def create_table():
    try:
        if request.method== 'POST':
            db = request.json['db_name']
            table_name=request.json['table_name']
            col1 = request.json['col1']
            col2 = request.json['col2']
            col3 = request.json['col3']
            col4 = request.json['col4']
            col1_type = request.json['col1_type']
            col2_type = request.json['col2_type']
            col3_type = request.json['col3_type']
            col4_type = request.json['col4_type']
            if(db=="my_sql"):
                config_db=config("localhost","my_sql","root","Kanchanpur12$")
                database= config_db.db_connect()
                cursor= database.cursor()
                # save edits
                query0 = "DROP TABLE IF EXISTS {}".format(table_name)
                cursor.execute(query0)
                query = ("CREATE TABLE {a}  ({b} {c},{d} {e},{f} {g},{h} {i} );".format(a=table_name,b=col1,c=col1_type,d=col2,e= col2_type,f=col3,g=col3_type,h=col4,i=col4_type))
                cursor.execute(query)
                database.close()
                lg.info('MySql_table is  created  succesfully !!')
                return jsonify('Table is  created  succesfully !!'+str(database))
            if (db == "cassandra_db"):
                config_cassandra = config_cassa("RSuNtUDywbhJRhXBouUPHcZx",
                                          "rlsSe5ok88KeaTYJ6JHyAoTbuE,LEJ6qKp5MusucJ4K+Bus1YLrs7HBGmOun1K+tDy0,kQ8MMbjj,5E1PnIg6NBLptXYkaXX6x7p0K-uofrxZhrA6kvBcZqR4TjiDROu")
                session = config_cassandra.cassandra_connect()
                #session = cassandra_connect()

                query0 = "DROP TABLE IF EXISTS jonty.{}".format(table_name)
                session.execute(query0)
                # save edits
                query1 = ("CREATE TABLE jonty.{a}({b} {c},{d} {e},{f} {g},{h} {i} );".format(a=table_name, b=col1, c=col1_type,d=col2, e=col2_type, f=col3,
                                                                                    g=col3_type, h=col4, i=col4_type))
                session.execute(query1).one()
                lg.info('Cassandra_table is  created  succesfully !!')
                return jsonify('Table is  created  succesfully !!' + str(session))

            if (db == "Mongo_db1"):
                config_mongodb = config_mongo("Mongo_db1")
                database = config_mongodb.mongodb_connect()
                #database = mongodb_connect()
                print(database)
                collection = database[(table_name)]
                record = {'col1': col1,
                          'col2': col2,
                          'col3': col3,
                          'col4': col4
                          }
                collection.insert_one(record)
                lg.info('Mongodb_table is  created  succesfully !!')
                return jsonify('Table is  created  succesfully !!' + str(collection))

    except Exception as e:
        lg.error("error has been occured.")
        lg.exception(str(e))




@app.route('/Insert_data',methods=['POST'])
def insertData():
    try:
        if request.method == 'POST':
            db = request.json['db_name']
            table_name = request.json['table_name']
            id = request.json['id']
            name = request.json['name']
            email = request.json['email']
            phone = request.json['phone']
            if (db == "my_sql"):
                config_db = config("localhost", "my_sql", "root", "Kanchanpur12$")
                database = config_db.db_connect()
                cursor = database.cursor()
                query = ("INSERT INTO {a} (id,name,email,phone) VALUES(%s,%s,%s,%s )".format(a=table_name))
                data=(id,name,email,phone)
                cursor.execute(query,data)
                database.commit()
                database.close()
                lg.info('MySql_table is  inserted  succesfully !!')
                return jsonify('Data  is  inserted  succesfully !!' )
            elif (db == "cassandra_db"):
                config_cassandra = config_cassa("RSuNtUDywbhJRhXBouUPHcZx",
                                                "rlsSe5ok88KeaTYJ6JHyAoTbuE,LEJ6qKp5MusucJ4K+Bus1YLrs7HBGmOun1K+tDy0,kQ8MMbjj,5E1PnIg6NBLptXYkaXX6x7p0K-uofrxZhrA6kvBcZqR4TjiDROu")
                session = config_cassandra.cassandra_connect()
                #session = cassandra_connect()
                # save edits
                query1 = ("INSERT INTO  jonty.{a}(id,name,email,phone) VALUES(%s,%s,%s,%s )".format(a=table_name))
                data = (id, name, email, phone)
                session.execute(query1,data)
                lg.info('Cassandra_table is  inserted  succesfully !!')
                return jsonify('Data is  inserted  succesfully !!' + str(session))
            elif (db == "Mongo_db1"):
                config_mongodb = config_mongo("Mongo_db1")
                database = config_mongodb.mongodb_connect()
                collection = database[(table_name)]
                record = {'id': id,
                          'name': name,
                          'email': email,
                          'phone': phone
                          }
                collection.insert_one(record)
                lg.info('Mongodb_table is  inserted  succesfully !!')

                return jsonify('Data  is  inserted   succesfully !!' + str(collection))
            else:
                lg.warning('database was not found.Please check again !!')

    except Exception as e:
        lg.error("error has been occured.")
        lg.exception(str(e))


@app.route('/Update_table',methods=['PUT'])
def updateData():
    try:
        if request.method =='PUT':
            db=request.json['db_name']
            table_name=request.json['table_name']
            id=(request.json['id'])
            name= (request.json['name'])
            email=(request.json['email'])
            if (db == "my_sql"):
                config_db = config("localhost", "my_sql", "root", "Kanchanpur12$")
                database = config_db.db_connect()
                cursor=database.cursor()
                query = "UPDATE {a} SET  name=%s, email=%s WHERE id=%s".format(a=table_name)
                data=(name,email,id)
                cursor.execute(query,data)
                database.commit()
                database.close()
                lg.info('MySql_table is  updated  succesfully !!')
                return jsonify('Table updated succesfully')
            elif (db == "cassandra_db"):
                config_cassandra = config_cassa("RSuNtUDywbhJRhXBouUPHcZx",
                                                "rlsSe5ok88KeaTYJ6JHyAoTbuE,LEJ6qKp5MusucJ4K+Bus1YLrs7HBGmOun1K+tDy0,kQ8MMbjj,5E1PnIg6NBLptXYkaXX6x7p0K-uofrxZhrA6kvBcZqR4TjiDROu")
                session = config_cassandra.cassandra_connect()
                # save edits
                query1 = ("UPDATE  jonty.{a} SET  name=%s, email=%s WHERE id=%s".format(a=table_name))
                data = ( name, email, id)
                session.execute(query1, data)
                lg.info('Cassandra_table is  updated  succesfully !!')
                return jsonify('Data is  updated  succesfully !!' + str(session))
            elif (db == "Mongo_db1"):
                config_mongodb = config_mongo("Mongo_db1")
                database = config_mongodb.mongodb_connect()
                collection = database[(table_name)]
                query={"id":{"$eq":id}}
                present_data = collection.find_one(query)
                new_data={'$set':{'name':name}}
                collection.update_many(present_data,new_data)
                new_data1={'$set':{'email':email}}
                collection.update_many(present_data, new_data1)
                lg.info('Mongodb_table is  updated  succesfully !!')
                return jsonify('Data  is  updated   succesfully !!' + str(collection))

            else:
                lg.warning('database was not found.Please check again !!')

    except Exception as e:

        lg.error("error has been occured.")
        lg.exception(str(e))


@app.route('/delete', methods=['DELETE'])
def deleteData():
    if (request.method=='DELETE'):
        try:
            db = request.json['db_name']
            table_name = request.json['table_name']
            id = request.json['id']
            if (db == "my_sql"):
                config_db = config("localhost", "my_sql", "root", "Kanchanpur12$")
                database = config_db.db_connect()
                cursor = database.cursor()
                query=("DELETE FROM {a} WHERE id= %s".format(a=table_name))
                data = (id,)
                cursor.execute(query,data)
                database.commit()
                respone = jsonify('Employee deleted successfully!')
                respone.status_code = 200
                lg.info('Mysql_Data is  deleted  succesfully !!')
                return respone
            elif (db == "cassandra_db"):
                config_cassandra = config_cassa("RSuNtUDywbhJRhXBouUPHcZx",
                                                "rlsSe5ok88KeaTYJ6JHyAoTbuE,LEJ6qKp5MusucJ4K+Bus1YLrs7HBGmOun1K+tDy0,kQ8MMbjj,5E1PnIg6NBLptXYkaXX6x7p0K-uofrxZhrA6kvBcZqR4TjiDROu")
                session = config_cassandra.cassandra_connect()
                # save edits
                query1 = ("DELETE FROM jonty.{a} WHERE id= %s".format(a=table_name))
                data = (id,)
                session.execute(query1, data)
                lg.info('Cassandra_Data is  deleted  succesfully !!')
                return jsonify('Data is  deleted   succesfully !!' + str(session))

            elif (db == "Mongo_db1"):
                config_mongodb = config_mongo("Mongo_db1")
                database = config_mongodb.mongodb_connect()
                collection = database[(table_name)]
                query= {"id":{"$eq":id}}
                collection.delete_one(query)
                lg.info('Mongodb_Data is  deleted  succesfully !!')
                return jsonify('Data  is  deleted  succesfully !!' + str(collection))

            else:
                lg.warning('database was not found.Please check again !!')
        except Exception as e:
            lg.error("error has been occured.")
            lg.exception(str(e))


@app.route('/bulkUpload',methods=['POST'])
def bulkUpload():
    try:
        if(request.method=='POST'):
            db = request.json['db_name']
            table_name = request.json['table_name']
            if (db == "my_sql"):

                config_db = config("localhost", "my_sql", "root", "Kanchanpur12$")
                database = config_db.db_connect()
                cursor = database.cursor()
                query = ("INSERT INTO {a} (id,name,email,phone) VALUES(%s,%s,%s,%s)".format(a=table_name))
                txtfile= open(r"C:\Users\sandesh\Desktop\person.txt","r")
                for row in txtfile:
                    data=row.split(',')
                    cursor.execute(query,[(data[0]),data[1],data[2], (data[3])])
                database.commit()
                cursor.close()
                respone = jsonify('bulkdata is uploaded  successfully!')
                respone.status_code = 200
                lg.info(' Bulk data in MySql_table is  uploaded   succesfully !!')
                return respone
            elif (db == "cassandra_db"):
                config_cassandra = config_cassa("RSuNtUDywbhJRhXBouUPHcZx",
                                                "rlsSe5ok88KeaTYJ6JHyAoTbuE,LEJ6qKp5MusucJ4K+Bus1YLrs7HBGmOun1K+tDy0,kQ8MMbjj,5E1PnIg6NBLptXYkaXX6x7p0K-uofrxZhrA6kvBcZqR4TjiDROu")
                session = config_cassandra.cassandra_connect()
                # save edits
                query1 = ("INSERT INTO jonty.{a} (id,name,email,phone) VALUES(%s,%s,%s,%s)".format(a=table_name))
                txtfile = open(r"C:\Users\sandesh\Desktop\person.txt", "r")
                for row in txtfile:
                    data=row.split(',')
                    print (data)
                    session.execute(query1,[(int(data[0])),data[1],data[2], int((data[3]))])
                    respone = jsonify('bulkdata is uploaded  successfully!')
                    respone.status_code = 200
                    lg.info(' Bulk data in cassandra_table is  uploaded   succesfully !!')
                    return respone
            elif (db == "Mongo_db1"):
                config_mongodb = config_mongo("Mongo_db1")
                database = config_mongodb.mongodb_connect()
                collection = database[(table_name)]
                txtfile = open(r"C:\Users\sandesh\Desktop\person.txt", "r")
                mongo_docs=[]
                counter= 0
                for row in txtfile:
                    record= []
                    data = row.split(',')
                    record = ({'id':  (int(data[0])),
                              'name':data[1],
                              'email': data[2],
                              'phone':(int( data[3]))
                              })
                    counter +=1
                    mongo_docs.insert(counter,record)
                collection.insert_many(mongo_docs)
                lg.info(' Bulk data in Mongodb_table is  uploaded   succesfully !!')
                return jsonify('Bulkdata  is uploaded succesfully !!' + str(collection))
            else:
                lg.warning('database was not found.Please check again !!')

    except Exception as e:
        lg.error("error has been occured.")
        lg.exception(str(e))


@app.route('/dumpdata',methods=['POST'])
def dumpData():
    try:
        if (request.method=='POST'):
            db = request.json['db_name']
            table_name = request.json['table_name']
            if (db == "my_sql"):
                config_db = config("localhost", "my_sql", "root", "Kanchanpur12$")
                database = config_db.db_connect()
                cursor = database.cursor()
                query=("SELECT * FROM {};").format(table_name)
                cursor.execute(query)
                result=cursor.fetchall()
                file=csv.writer(open(r'dbdump.csv','w'))
                for data in result:
                    file.writerow(data)
                database.commit()
                cursor.close()
                respone = jsonify('bulkdata is downloaded  successfully!')
                respone.status_code = 200
                lg.info(' Bulk data from MySql_db is  downloaded  succesfully !!')
                return respone
            elif (db == "cassandra_db"):
                config_cassandra = config_cassa("RSuNtUDywbhJRhXBouUPHcZx",
                                                "rlsSe5ok88KeaTYJ6JHyAoTbuE,LEJ6qKp5MusucJ4K+Bus1YLrs7HBGmOun1K+tDy0,kQ8MMbjj,5E1PnIg6NBLptXYkaXX6x7p0K-uofrxZhrA6kvBcZqR4TjiDROu")
                session = config_cassandra.cassandra_connect()
                query1 = ("SELECT * FROM jonty.{};").format(table_name)
                result= session.execute(query1)
                #result = session.fetchall()
                file = csv.writer(open(r'cassandradump.csv', 'w'))
                for data in result:
                    file.writerow(data)
                respone = jsonify('bulkdata is downloaded  successfully!')
                respone.status_code = 200
                lg.info(' Bulk data from Cassandra_db is  downloaded  succesfully !!')
                return respone
            elif (db == "Mongo_db1"):
                config_mongodb = config_mongo("Mongo_db1")
                database = config_mongodb.mongodb_connect()
                collection = database[(table_name)]
                tables= database.list_collection_names()
                data = collection.find()
                for info in data:
                    file = csv.writer(open(r'Mongodb.csv', 'w'))
                    file.writerow((data))
                respone = jsonify('bulkdata is downloaded  successfully!')
                respone.status_code = 200
                lg.info(' Bulk data from Mongo_table is  downloaded  succesfully !!')
                return respone
            else:
                lg.warning('database was not found.Please check again !!')

    except Exception as e:
        lg.error("error has been occured.")
        lg.exception(str(e))


if __name__ == "__main__":
    app.debug = True
    app.run()