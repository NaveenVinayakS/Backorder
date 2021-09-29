import pandas
from file_operations import file_methords
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation


class prediction:

    def __init__(self,path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.pred_data_val = Prediction_Data_validation(path)

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile()
            self.log_writer.log(self.file_object,'Start of Prediction')
            
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            
            data_output=data_getter.get_data()
            data = data_output.copy()
            print("get_data",data)
            
            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)

            #data = preprocessor.remove_columns(data, ["sku","oe_constraint"])
            
            data = preprocessor.encodeCategoricalValuesPred(data)
            print("encode",data)
            #is_null_present=preprocessor.is_null_present(data)
            
            #if(is_null_present):
                #data = data.dropna()
            data = data.dropna()
            print("drop na",data)
            data = preprocessor.scale_numerical_columns(data)
            print("scale",data)
            data = preprocessor.pcaTransformation(data)
            print("PCA",data)
            file_loader=file_methords.File_Operation(self.file_object,self.log_writer)
            
            model_name = file_loader.find_correct_model_file()

            model = file_loader.load_model(model_name)
            
            result_list=list(model.predict(data))
            
            result = pandas.DataFrame(result_list, columns=['Prediction'])
            
            result["Prediction"] = result["Prediction"].map({ 0 : "Yes", 1: "No"})
            
            path="Prediction_Output_File/Predictions.csv"
            
            result.to_csv("Prediction_Output_File/Predictions.csv",header=True,mode='a+')

            data_output['Prediction']=result_list
            data_output["Prediction"] = data_output["Prediction"].map({0: "Backorder", 1: "No Backorder"})
            data_output.to_csv("Prediction_Output_File/Predictions_final.csv",index=False ,header=True)

            print("Final data to concat :-",data_output)

            
            self.log_writer.log(self.file_object,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
