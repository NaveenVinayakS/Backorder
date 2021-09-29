from datetime import datetime
from os import listdir
import pandas
from application_logging.logger import App_Logger

class dataTransform():
    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()
    
    def replaceMissingWithNull(self):
        log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pd.read_csv(self.goodDataPath+"/" + file)
                csv.fillna("'NULL'",inplace=True)
                csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                self.logger.log(log_file," %s: File Transformed successfully!!" % file)
        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            log_file.close()
        log_file.close()
        
    def addQuotesToStringValuesInColumn(self):
        
        log_file = open("Training_Logs/addQuotesToStringValuesInColumn.txt",'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data = pd.read_csv(self.goodDataPath+"/" + file)
                str_column = ["potential_issue", "deck_risk", "oe_constraint", "ppap_risk", "stop_auto_buy", "rev_stop",
                                        "went_on_backorder"]
                for col in data.columns:
                     if col in str_column:
                            data[col] = data[col].apply
                data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                self.logger.log(log_file,"%s : Quotes added successfully !!"%file)
        
        except Exception as e:
            
            self.logger.log(log_file,"Data Transformation Failed because :: %s" %e)
            log_file.close()
        log_file.close()
                