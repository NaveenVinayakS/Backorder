from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger

class Prediction_Data_validation:
    def __init__(self,path):
        self.Batch_Dictionary = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()
        #self.object_log = logger.App_Logger()
        
    def valuesFromSchema(self):
        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic["SampleFileName"]
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']
            
            file_object = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file_object,"LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n")
            file_object.close()
            
        except Exception as e:
            file_object = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file_object,e)
            file_object.close()
            
        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns
            
    def manualRegexCreation(self):
        regex = "file_Training_batchs.csv"
        return regex
    def createDirectoryForGoodBadRawData(self):
        try:
            path = os.path.join('Prediction_Raw_Files_Validated/','Good_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join('Prediction_Raw_Files_Validated/','Bad_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
                
        except:
            file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file_object,'Error in creating Folder for Good_Bad_Raw')
            file_object.close()
            
    def deleteExistingGoodDataTrainingFolder(self):
        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file_object,'Good_Raw folder is deleted successfully')
                file_object.close()                
        except:
            file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file_object,'Error in Deleting Good_Raw')
            file_object.close()
            
    def deleteExistingBadDataTrainingFolder(self):
        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file_object,'Bad_Raw folder is deleted successfully')
                file_object.close()                
        except:
            file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file_object,'Error in Deleting Bad_Raw')
            file_object.close()
            
    def moveBadFilesToArchiveBad(self):
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H_%M_%S")
        try:
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            if os.path.isdir(source):
                path='PredictionArchivedBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                folder_name = 'PredictionArchivedBadData/BadData_' + str(date)+"_"+str(time)
                if os.path.isdir(folder_name):
                    os.makedirs(folder_name)
                files = os.listdir(source)
                for i in files:
                    if i not in os.listdir(folder_name):
                        shutil.move(source+i,folder_name)
                file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file_object,'Bad files moved to Bad_Archive')
                file_object.close()
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file_object,'Bad files from Bad_Raw moved to Bad_Archive')
                file_object.close()
        except:
            file_object = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file_object,'Error in moving Bad file to Archive')
            file_object.close()
            
            
    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        
        onlyfiles = [f for f in listdir(self.Batch_Dictionary)]
        print('HI')
        
        try:
            file_object = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                print('filename :-',filename)
                if (re.match(regex, filename)):
                    print('matched')
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    print("test:",splitAtDot)
                    #print('split :- ',splitAtDot[1])
                    print('Len::',splitAtDot)
                    print("Len :-",LengthOfDateStampInFile)
                    print(len(splitAtDot[1]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        print(len(splitAtDot[2]))
                        print('time :-',LengthOfTimeStampInFile)
                        print('len :-',len(splitAtDot[2]))
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            print("enter")
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(file_object,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)
            
                        else:
                            shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(file_object,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(file_object,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Prediction_Batch_files/" + filename, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(file_object, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            file_object.close()
        except Exception as e:
            file_object = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file_object, "Error occured while validating FileName %s" % e)
            file_object.close()
            
    def validateColumnLength(self,NumberofColumns):
        try:
            file_object = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            for i in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv('Prediction_Raw_Files_Validated/Good_Raw/'+i)
                print(csv.shape[1])
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + i, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(file_object, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % i)
            self.logger.log(file_object, "Column length validation completed")
            file_object.close()
        except Exception as e:
            file_object = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file_object, e)
            file_object.close()

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')
            
    def validateMissingValuesInWholeColumn(self):
        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")

            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Index"}, inplace=True)
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                    
        except Exception as e:
            file_object = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(file_object, e)
            file_object.close()            