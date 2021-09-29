from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from catboost import CatBoostClassifier
from sklearn.metrics  import roc_auc_score,accuracy_score

class Model_Finder:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.clf = RandomForestClassifier()
        self.xgb = XGBClassifier(objective='binary:logistic')
        self.cat = CatBoostClassifier(iterations=2)
        
    def get_best_params_for_random_forest(self,train_x,train_y):
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_random_forest method ')
        try:
            self.param_grid = {"n_estimators": [10, 50, 100, 130], "criterion": ['gini', 'entropy'],
                               "max_depth": range(2, 4, 1), "max_features": ['auto', 'log2']}
            self.grid = GridSearchCV(estimator=self.clf, param_grid=self.param_grid, cv=5,  verbose=3)
            
            self.grid.fit(train_x, train_y)
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']
            self.clf = RandomForestClassifier(n_estimators=self.n_estimators, criterion=self.criterion,max_depth=self.max_depth, max_features=self.max_features)
            self.clf.fit(train_x, train_y)
            self.logger_object.log(self.file_object, 'Random Forest best params: '+str(self.grid.best_params_)+'.get_best_params_for_random_forest completed')
            return self.clf
        
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in get_best_params_for_random_forest methord :- ' + str(e))
    def get_best_params_for_xgboost(self,train_x,train_y):
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_XGBoost method ')
        try :
            self.param_grid_xgboost = {'learning_rate': [0.5, 0.1, 0.01, 0.001],'max_depth': [3, 5, 10, 20],'n_estimators': [10, 50, 100, 200]}
            self.grid= GridSearchCV(XGBClassifier(objective='binary:logistic'),self.param_grid_xgboost, verbose=3,cv=5)
            self.grid.fit(train_x, train_y)
            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']
            
            self.xgb = XGBClassifier(learning_rate=self.learning_rate, max_depth=self.max_depth, n_estimators=self.n_estimators)
            self.xgb.fit(train_x, train_y)
            self.logger_object.log(self.file_object,'XGBoost best params: ' + str(  self.grid.best_params_) + 'get_best_params_for_xgboost Completed')
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in get_best_params_for_xgboost methord :- ' + str(e))

    def get_best_params_for_catboost(self,train_x,train_y):
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_catboost method ')
        try :
            self.param_grid_catboost = {'learning_rate': [0.03, 0.1],'depth': [4, 6, 10],'l2_leaf_reg': [1, 3, 5, 7, 9]}
            
               
            self.grid= GridSearchCV(estimator=self.cat, param_grid=self.param_grid_catboost, cv=5,  verbose=3)
            
            
            self.grid.fit(train_x, train_y)
            self.learning_rate = self.grid.best_params_['learning_rate']
            self.depth = self.grid.best_params_['depth']
            self.l2_leaf_reg = self.grid.best_params_['l2_leaf_reg']
            
            self.cat = XGBClassifier(learning_rate=self.learning_rate, depth=self.depth, l2_leaf_reg=self.l2_leaf_reg)
            self.cat.fit(train_x, train_y)
            self.logger_object.log(self.file_object,'Catboost best params: ' + str(  self.grid.best_params_) + 'get_best_params_for_catboost Completed')
            return self.cat
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in get_best_params_for_catboost methord :- ' + str(e))
  
    def get_best_model(self,train_x,train_y,test_x,test_y):
        self.logger_object.log(self.file_object,'Entered the get_best_model method')
    
        try:
            self.xgboost= self.get_best_params_for_xgboost(train_x,train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x) 

            if len(test_y.unique()) == 1: 
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))  
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost) 
                self.logger_object.log(self.file_object, 'AUC for XGBoost:' + str(self.xgboost_score))

            self.random_forest=self.get_best_params_for_random_forest(train_x,train_y)
            self.prediction_random_forest=self.random_forest.predict(test_x)

            if len(test_y.unique()) == 1:
                self.random_forest_score = accuracy_score(test_y,self.prediction_random_forest)
                self.logger_object.log(self.file_object, 'Accuracy for RF:' + str(self.random_forest_score))
            else:
                self.random_forest_score = roc_auc_score(test_y, self.prediction_random_forest) 
                self.logger_object.log(self.file_object, 'AUC for RF:' + str(self.random_forest_score))

            #comparing the two models
            if(self.random_forest_score <  self.xgboost_score):
                return 'XGBoost',self.xgboost
            else:
                return 'RandomForest',self.random_forest

        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in get_best_model method.Message:  ' + str(e))
