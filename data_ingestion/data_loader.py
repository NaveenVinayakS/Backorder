import pandas as pd

class Data_Getter:
    def __init__ (self, file_object, logger_object):
        self.training_file='Training_FileFromDB/InputFile.csv'
        self.file_object=file_object
        self.logger_object = logger_object
        
    def get_data(self):
        self.logger_object.log(self.file_object,'Entered the get_data methord for reading the CSV file')
        try:
            self.data = pd.read_csv(self.training_file)
            self.logger_object.log(self.file_object,'file readed successfully and Dataframe created !!')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in reading the CSV file in get_data methord : '+str(e))