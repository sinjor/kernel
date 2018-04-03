from sklearn import datasets
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn import metrics
import xgboost as xgb
import pandas as pd
import numpy as np

#print(instance.read().decode('gb2312'))
train_data = pd.read_csv("GermanCredit/train.csv")
test_data = pd.read_csv("GermanCredit/test.csv")
#dict_data = pd.read_csv("GermanCredit/dict.csv")

#变量onehot(使用了pandas自带的get_dummies)
is_factor = (train_data.dtypes == object) * 1
train_data.loc[:, is_factor == 1]
numeric_data = train_data.loc[:, is_factor == 0]
one_hot_data = train_data.loc[:, is_factor == 1]
factor_data = pd.get_dummies(one_hot_data, columns=one_hot_data.columns)
feature_train_data = pd.concat([numeric_data, factor_data], axis=1)
#print(feature_data)

test_data.loc[:, is_factor == 1]
numeric_data = test_data.loc[:, is_factor == 0]
one_hot_data = test_data.loc[:, is_factor == 1]
factor_data = pd.get_dummies(one_hot_data, columns=one_hot_data.columns)
feature_test_data = pd.concat([numeric_data, factor_data], axis=1)

data  = feature_train_data.iloc[:, 2:]
#print(data.columns)
label = feature_train_data.Target
#随机将数据集划分成训练集和验证集
train_x, test_x, train_y, test_y = train_test_split(data, label, train_size = 0.7, random_state=0)

train_data = train_x
train_label = train_y
test_data = test_x
#生成xgboost可以识别的训练数据和验证数据
dtrain = xgb.DMatrix(data=train_data, label=train_label)
dtest = xgb.DMatrix(data=test_data)
#xgb的超参数
params={'booster':'gbtree',
        
    'eta': 0.1,
    'n_estimators':55,
    'max_depth':3, #3 10
    'min_child_weight':1,#1 6
    'gamma':0.3,
    'subsample':0.75,
    'colsample_bytree':0.75,
    'objective': 'binary:logistic',
    'nthread':8,
    'scale_pos_weight':2,
    'eval_metric': 'auc',
    'lambda':0,
    'seed':1,
    'silent':0}

watchlist = [(dtrain,'train')]
#bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=100, evals=watchlist)
# for i in range(3, 10, 2):
#     for j in range(1, 6, 2):
#         print(i,j)
#         params["max_depth"] = i
#         params["min_child_weight"] = j
# for i in range(0, 5):
#     params["gamma"] = i/10.0 
#     print(params["gamma"])

bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=55)
test_pred = bst.predict(data=dtest)
test_pred_y = (test_pred >=0.5)*1
#test_pred_y[2] = 1
auc_prob = metrics.roc_auc_score(y_true=test_y, y_score=test_pred)
auc_class = metrics.roc_auc_score(y_true=test_y, y_score=test_pred_y)
#print(test_pred)
#print(test_pred_y)
print("auc_prob:", auc_prob)
print("auc_class:", auc_class)
#测试集进行验证
dtest = xgb.DMatrix(data=feature_test_data.iloc[:, 2:])
test_y = feature_test_data.Target
test_pred = bst.predict(data=dtest)
test_pred_y = (test_pred >=0.35)*1
auc_prob = metrics.roc_auc_score(y_true=test_y, y_score=test_pred)
auc_class = metrics.roc_auc_score(y_true=test_y, y_score=test_pred_y)
print("auc_prob:", auc_prob)
print("auc_class:", auc_class)

def xgb_cv():
    params={'booster':'gbtree',
            
        'eta': 0.01,
        'n_estimators':100,
        'max_depth':3,
        'min_child_weight':1,
        'gamma':0,
        'subsample':0.75,
        'colsample_bytree':0.75,
        'objective': 'binary:logistic',
        'nthread':8,
        'scale_pos_weight':2,
            
        'eval_metric': 'auc',
        'lambda':0,
        'seed':1,
        'silent':0}
    xgb_cv_result = xgb.cv(params=params, dtrain=dtrain, num_boost_round=1000, nfold = 5, metrics='auc', early_stopping_rounds=50)
    print(xgb_cv_result)
    params["n_estimators"] = xgb_cv_result.shape[0]