#20180403
#v1.0 没有进行特征工程，提取numeric和factor(onehot)使用xgboos简单调参后建模
#线下分数：logloss_prob: 0.08956929329219732
#线上分数：0.08228

from sklearn import datasets
from sklearn.cross_validation import train_test_split
from sklearn import metrics
import xgboost as xgb
import pandas as pd
import numpy as np

test_data = pd.read_csv("G:/program_monkey/ML/tianchi/alimama_ad/test.txt",sep=" ")
train_data = pd.read_csv("G:/program_monkey/ML/tianchi/alimama_ad/train.txt",sep=" ")
factor_data = ["item_category_list","user_gender_id","user_occupation_id"]
numeric_data =["item_price_level","item_sales_level","item_collected_level",
               "item_pv_level","user_age_level","user_star_level","shop_review_num_level",
               "shop_review_positive_rate","shop_star_level","shop_score_service",
               "shop_score_delivery","shop_score_description"]

factor_feature_data = pd.get_dummies(train_data[factor_data], columns=factor_data)
numeric_feature_data = train_data[numeric_data]
feature_data = pd.concat([factor_feature_data, numeric_feature_data], axis=1)

factor_feature_data = pd.get_dummies(test_data[factor_data], columns=factor_data)
numeric_feature_data = test_data[numeric_data]
test_feature_data = pd.concat([factor_feature_data, numeric_feature_data], axis=1)

data  = feature_data
label = train_data.is_trade

#随机将数据集划分成训练集和验证集
train_x, test_x, train_y, test_y = train_test_split(data, label, train_size = 0.7, random_state=0)

#生成xgboost可以识别的训练数据和验证数据
dtrain = xgb.DMatrix(data=train_x, label=train_y)
dtest = xgb.DMatrix(data=test_x)
#xgb的超参数
params={'booster':'gbtree',
    'eta': 0.1,
    'n_estimators':103,
    'max_depth':5, #3 10
    'min_child_weight':3,#1 6
    'gamma':0.3,
    'subsample':0.8,
    'colsample_bytree':0.8,
    'objective': 'binary:logistic',
    'nthread':8,
    'scale_pos_weight':1,
    'eval_metric': 'logloss',
    'lambda':0,
    'seed':0,
    'silent':0}

watchlist = [(dtrain,'train')]
#bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=103, evals=watchlist)
bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=103)
test_pred = bst.predict(data=dtest)
logloss_prob = metrics.log_loss(y_true=test_y, y_pred=test_pred)
print("logloss_prob:",logloss_prob)

dtest = xgb.DMatrix(data=test_feature_data) 
test_pred = bst.predict(data=dtest)

result = test_data[["instance_id"]]
#print(type(result))
result["predicted_score"] = test_pred
print(result.head())
result.to_csv("result3.csv", sep=" ", index=False, line_terminator='\n')