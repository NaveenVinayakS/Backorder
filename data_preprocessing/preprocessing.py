import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import joblib

class Preprocessor:
    def __init__ (self, file_object, logger_object):
        
        self.file_object=file_object
        self.logger_object = logger_object
        
    def encodeCategoricalValues(self,data):
        str_col = ["potential_issue","deck_risk","oe_constraint","ppap_risk","stop_auto_buy","rev_stop","went_on_backorder"]
        for column in str_col:
            data[column] = data[column].map({"Yes" : 0, "No" : 1})
        return data

    
    def remove_columns(self,data,columns):
        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        self.data = data
        self.columns = columns
        try:
            self.useful_data=self.data.drop(self.columns, axis=1) # drop the labels specified in the columns
            self.logger_object.log(self.file_object,'Columns removal successful')
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in remove_columns Exception message:  '+str(e))
    
    def seperate_label_feature(self,data, label_column_name):
        
        self.logger_object.log(self.file_object, 'Entered the seperate_label_feature method of the Preprocessor class')
        try:
            self.X = data.drop(labels=label_column_name,axis=1)
            self.Y = data[label_column_name]
            self.logger_object.log(self.file_object,'create X and Y Successful.')
            return self.X,self.Y
        
        except Exception as e:
            self.logger_object.log(self.file_object, 'Error occured in  creating X and Y in seperate_label_feature' + str(e))
        
    def is_null_present(self,data):
        self.logger_object.log(self.file_object, 'Entered the is_null_present method')
        try:
            self.null_counts=data.isna().sum()
            for i in self.null_counts:
                if i>0:
                    self.null_present=True
                    break
            if(self.null_present):
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present
        
        except Exception as e:
            self.logger_object.log(self.file_object, 'Error occured in is_null_present method :- '+str(e))
    
    def get_columns_with_zero_std_deviation(self,data):
        self.logger_object.log(self.file_object,'Entered the get_columns_with_zero_std_deviation')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                if (self.data_n[x][std]==0):
                    self.col_to_drop.append(x)
                self.logger_object.log(self.file_object,'zero standard deviation column is found succesfully in get_columns_with_zero_std_deviation')
            return self.col_to_drop
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in get_columns_with_zero_std_deviation method . message:  ' + str(e))
    
    def scale_numerical_columns(self,data):
        self.logger_object.log(self.file_object,'Entering the scaled numerical column')
        self.data=data

        self.num_df = self.data.drop(["potential_issue","deck_risk","ppap_risk","stop_auto_buy","rev_stop"],axis=1)
        print("Num_df",self.num_df.shape)
        try:
            self.scaler = joblib.load('data_preprocessing/std_scalar.pkl')
            print("going to scale")
            print("\n")
            #self.scaler = StandardScaler()
            self.scaled_data = self.scaler.transform(self.num_df)
            print("self_scaled data",self.scaled_data)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns,index=self.data.index)
            self.data.drop(columns=self.scaled_num_df.columns, inplace=True)
            self.data = pd.concat([self.scaled_num_df, self.data], axis=1)
            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, 'Error occured in scaling the data in scale_numerical_columns methord :' +str(e))    
    
    def pcaTransformation(self,X_scaled_data):
        try:
            self.data = X_scaled_data
            self.num_df = self.data.drop(["potential_issue", "deck_risk", "ppap_risk", "stop_auto_buy", "rev_stop"], axis=1)

            pca = joblib.load('data_preprocessing/PCA.pkl')
            #pca = PCA(n_components =7)

            new_data = pca.transform(self.num_df)
            print("PCA",new_data)
            principal_x = pd.DataFrame(new_data,columns=['PC-1', 'PC-2', 'PC-3', 'PC-4', 'PC-5', 'PC-6', 'PC-7'],index=self.num_df.index)
            self.data.drop(columns=self.num_df.columns, inplace=True)
            self.data = pd.concat([principal_x, self.data], axis=1)
        # must save this PCA and use that for test data
            return self.data
        except Exception as e:
            print(e)
    
    def encodeCategoricalValuesPred(self,data):
        
        str_col = ["potential_issue", "deck_risk", "oe_constraint", "ppap_risk", "stop_auto_buy", "rev_stop"]
        for column in str_col:
            data[column] = data[column].map({"Yes" : 0, "No" : 1})
        return data
    def encodeCategoricalValuesPrediction(self, data):
        str_col = ["potential_issue", "deck_risk", "oe_constraint", "ppap_risk", "stop_auto_buy", "rev_stop"]
        for column in str_col:
            data[column] = data[column].map({"Yes": 0, "No": 1})

        return data                         