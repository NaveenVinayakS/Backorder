import shutil
import pandas as pd
from datetime import datetime
from os import listdir
import os
import csv
from application_logging.logger import App_Logger
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class dBOperation:
    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self,keyspace):
        try:
            cloud_config= {
                    'secure_connect_bundle': r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\secure-connect-backorder.zip'
            }
            auth_provider = PlainTextAuthProvider('mKaLohoHzBFYelvIeeKlfHlP', 'n4DlDmjgHrHYs3_JrYCgY_BJeLmGxum3f3.8puUzP.xaFPyMgx8OE1cW2Oj.O,,ylD52y57i5L,Ax+TMzpq1K3iE2FqoJQcMvZlpjPy3TfvXydQZMk6PSk2FzUgXZyFX')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect(keyspace)
            
            row = session.execute("select release_version from system.local").one()
            if row:
                print(row[0])
            else:
                print("An error occurred.")
            file_object = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file_object, "Opened database connection successfully")
            file_object.close()
        except:
            file_object = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file_object, "Error in Establishing database ")
            file_object.close()
        return session
    
    def createTableDB(self,keyspace,column_names):
        try:
            create = ''
            conn = self.dataBaseConnection(keyspace)
            res = conn.execute("SELECT table_name FROM system_schema.tables;")
            for i in res:
                if i[0]=='Good_Raw_Data_Prediction':
                    create = 'Available'
                else:
                    create = 'Not Available'
            print(create)
            if create == 'Available':
                file_object = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file_object, "Tables created successfully!!")
                file_object.close()
            else:
                self.col_nam = list(column_names.keys())
                print(self.col_nam)
                self.dtype_col = list(column_names.values())
                conn.execute("CREATE TABLE IF NOT EXISTS "+keyspace+".Good_Raw_Data_Prediction ("+self.col_nam[0]+" int PRIMARY KEY);")
                #conn.execute("CREATE TABLE IF NOT EXISTS "+keyspace+".test ("+self.col_nam[0]+" int PRIMARY KEY);")
                
                for val,dtype in zip(self.col_nam[1:],self.dtype_col[1:]):
                    try:
                        #print(val)
                        conn.execute("alter table backorder.Good_Raw_Data_Prediction add "+ val +" "+dtype)
                    except:
                        pass
                    #conn.close()
                    file_object = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                    self.logger.log(file_object, "Tables created successfully!!")
                    file_object.close()
        
                    
                    file_object = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                    self.logger.log(file_object, "DB connection closed!!")
                    file_object.close()
                
        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()

    def insertIntoTableGoodData(self,keyspace):
        #print("enter")
        #print("col_nam",self.col_nam)
        conn = self.dataBaseConnection(keyspace)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        file_object = open("Prediction_Logs/DbInsertLog.txt", 'a+')
        
        for file in onlyfiles:
            try:
                print('hi')
                df = pd.read_csv(goodFilePath+"/"+file)
                for i in df.index:
                    #csv reader object
                    print(df[i:i+1].values.tolist()[0])
                    # removing quotes from the list
                    query = "insert into Good_Raw_Data_Prediction "+("[{0}]".format(', '.join(map(str, df.columns))))+" values {val}".format(col=tuple(df.columns),val=tuple(df[i:i+1].values.tolist()[0]))
                    query = query.replace('[','(')
                    query = query.replace(']',')')
                    print(query)
                    #session.execute("insert into good_raw_data"+("[{0}]".format(', '.join(map(str, df.columns))))+" values {val}".format(col=tuple(df.columns),val=tuple(df[i:i+1].values.tolist()[0])))
                    conn.execute(query)
                shutil.copy(goodFilePath+'/'+file, 'Prediction_FileFromDB')
                os.rename('Prediction_FileFromDB'+'/'+file,'Prediction_FileFromDB'+'/'+"InputFile.csv")
                self.logger.log(file_object," %s: File loaded successfully!!" % file)

            except Exception as e:
                #conn.rollback()
                print("error :-",e)
                self.logger.log(file_object,"Error while Inserting table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(file_object, "File Moved Successfully %s" % file)
                file_object.close()
                
    
                