import pandas as pd
import numpy as np
back_order = pd.read_csv("InputFile.csv")
print(back_order.shape)
back_order.head()

import joblib

###### TEST DATA ########
print(back_order[back_order['perf_6_month_avg']==-99].shape)
print(back_order[back_order['perf_12_month_avg']==-99].shape)

back_order["perf_6_month_avg"].replace({-99:np.nan}, inplace=True)
back_order['perf_12_month_avg'].replace({-99:np.nan}, inplace=True)

print(back_order[back_order['perf_6_month_avg']==-99].shape)
print(back_order[back_order['perf_12_month_avg']==-99].shape)
print(back_order.shape)

back_order = back_order.dropna()
print(back_order.shape)


str_col = ["potential_issue","deck_risk","oe_constraint","ppap_risk","stop_auto_buy","rev_stop"]
for column in str_col:
    back_order[column] = back_order[column].map({"Yes" : 0, "No" : 1})



def scale_numerical_columns(data):
    #logger_object.log(self.file_object,'Entering the scaled numerical column')
    data=data
    num_df = data.drop(["potential_issue","deck_risk","ppap_risk","stop_auto_buy","rev_stop"],axis=1)
    print("Num_df",num_df.shape)
    scaler = joblib.load(r'C:\Users\z030590\PycharmProjects\Project\data_preprocessing\std_scalar.pkl')
    print("going to scale")
    print("\n")
    #self.scaler = StandardScaler()
    scaled_data = scaler.transform(num_df)
    print("self_scaled data",scaled_data)
    scaled_num_df = pd.DataFrame(data=scaled_data, columns=num_df.columns,index=data.index)
    data.drop(columns=scaled_num_df.columns, inplace=True)
    data = pd.concat([scaled_num_df, data], axis=1)
    #self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
    return data



scale_numerical_columns(back_order)

