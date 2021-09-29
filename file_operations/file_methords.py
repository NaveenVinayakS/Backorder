import pickle
import os
import shutil
from pathlib import Path
import joblib

class File_Operation:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory='models/'

        self.root_path = Path().absolute()
        self.root_path = str(self.root_path)
        self.root_path = self.root_path + '\models'


    def save_model(self,model,filename):
        self.logger_object.log(self.file_object, 'Entered the save_model method of the File_Operation class')
        try:
            self.root_path = Path().absolute() 
            self.root_path  = str(self.root_path)
            self.root_path = self.root_path +'\models'
            
            joblib.dump(model, self.root_path+filename+'.pkl')
            self.logger_object.log(self.file_object,'Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')

            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            

    def load_model(self,filename):
        self.logger_object.log(self.file_object, 'Entered the load_model method of the File_Operation class')
        try:
            CA = joblib.load(self.root_path+'\\'+filename+'.pkl')
            self.logger_object.log(self.file_object,'Model File loaded')
            return CA
        except Exception as e:
            self.logger_object.log(self.file_object,'Error in loading the mode' + str(e))
           

    def find_correct_model_file(self):
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            #self.cluster_number= cluster_number
            self.folder_name=self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    #if (self.file.index(str( self.cluster_number))!=-1):
                    self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(e))