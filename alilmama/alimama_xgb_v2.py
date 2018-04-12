#20180403
#v1.0 没有进行特征工程，提取numeric和factor(onehot)使用xgboos简单调参后建模
#线下分数(随机划分)：logloss_prob: 0.08956929329219732
#last day:0.08253
#线上分数：0.08280
#
#20180404
#v2.0 在v1.0基础上加入了广告类目属性和上下文预测的类目属性对比特征集
#线下分数(随机划分)：logloss_prob: 0.08961649265749788
#last day:0.08243
#线上分数：0.08269
#20180410
#v2.1在4号的基础上对所有数据重新建模（不设验证集），相当于用7天预测最后一天，效果有所提升
#线上分数：0.08217
#1.说明广告类目属性对比特征集起到作用了
#2.不能用前6天的数据预测第8天（线上那一天），可以尝试用线上那天之前的6天进行预测
#
#20180411
# v2.2在v2.1的基础上用上线那天之前的6天进行预测
# 线下分数: 0.08243
# 线上分数：0.08211
# #20180412
# v3.0 加入了部分商店特征（转化率），出现过拟合，初步原因可能是使用了预测数据集的商店特征（预先知道了是否要购买广告）
# 线下分数: 0.07922
# 线上分数：0.08953
# 
from sklearn import datasets
from sklearn.cross_validation import train_test_split
from sklearn import metrics
import xgboost as xgb
import pandas as pd
import numpy as np
import time

test_data = pd.read_csv("G:/program_monkey/ML/tianchi/alimama_ad/test.txt",sep=" ")
train_data = pd.read_csv("G:/program_monkey/ML/tianchi/alimama_ad/train.txt",sep=" ")
factor_data = ["item_category_list","user_gender_id","user_occupation_id"]
numeric_data =["item_price_level","item_sales_level","item_collected_level",
               "item_pv_level","user_age_level","user_star_level","shop_review_num_level",
               "shop_review_positive_rate","shop_star_level","shop_score_service",
               "shop_score_delivery","shop_score_description"]
timestamp = "context_timestamp"
train_data[[timestamp,"is_trade"]]
feature_begin = 3
#last_day_begin_time = "2018-09-24 00:00:00"
time_array = time.strptime("2018-09-24 00:00:00", "%Y-%m-%d %H:%M:%S")
last_day_timestamp = int(time.mktime(time_array))
#second_day_begin_time = "2018-09-19 00:00:00"
time_array = time.strptime("2018-09-19 00:00:00", "%Y-%m-%d %H:%M:%S")
second_day_timestamp = int(time.mktime(time_array))



def xgb_build(params, train_feature_data, num_boost_round):
    #前6天作为训练集，最后一天作为验证集
    train_x = train_feature_data.ix[train_feature_data[timestamp] < last_day_timestamp, feature_begin:]
    test_x = train_feature_data.ix[train_feature_data[timestamp] >= last_day_timestamp, feature_begin:]
    train_y = train_feature_data.loc[train_feature_data[timestamp] < last_day_timestamp, ["is_trade"]]
    test_y = train_feature_data.loc[train_feature_data[timestamp] >= last_day_timestamp, ["is_trade"]]

    #生成xgboost可以识别的训练数据和验证数据
    dtrain = xgb.DMatrix(data=train_x, label=train_y)
    dtest = xgb.DMatrix(data=test_x)

    watchlist = [(dtrain,'train')]
    bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=num_boost_round, evals=watchlist)
    test_pred = bst.predict(data=dtest)
    logloss_prob = metrics.log_loss(y_true=test_y, y_pred=test_pred)
    print("logloss_prob:",logloss_prob)
    print("xgb build success")
    return bst

def online_pred(xgb_bst, test_feature_data, result_file):
    dtest = xgb.DMatrix(data=test_feature_data) 
    test_pred = xgb_bst.predict(data=dtest)

    result = test_data[["instance_id"]]
    result["predicted_score"] = test_pred
    result.to_csv(result_file, sep=" ", index=False, line_terminator='\n')
    print("online_pred success")
    print("result write to %s"%result_file)
    
def online_rebuild_pred(train_feature_data, test_feature_data, params, num_boost_round, result_file):
    #global factor_data
    train_x = train_feature_data.ix[train_feature_data[timestamp] > second_day_timestamp, feature_begin:]
    train_y = train_feature_data.loc[train_feature_data[timestamp] > second_day_timestamp, ["is_trade"]]

    #train_x = train_feature_data.ix[:, 2:]
    #train_y = train_feature_data.loc[:, ["is_trade"]]
    dtrain = xgb.DMatrix(data=train_x, label=train_y)
    dtest = xgb.DMatrix(data=test_feature_data) 
    watchlist = [(dtrain,'train')]
    bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=num_boost_round, evals=watchlist)
    test_pred = bst.predict(data=dtest)

    result = test_data[["instance_id"]]
    result["predicted_score"] = test_pred
    #print(result.head())
    result.to_csv(result_file, sep=" ", index=False, line_terminator='\n')
    print("online_rebuild_pred success")
    print("result write to %s"%result_file)
    
def get_feature(input_data, data_type, csv_read=True, csv_write=False):
    if data_type == "train":
        category_feature_file = "train_category_feature.csv"
    elif data_type == "test":
        category_feature_file = "test_category_feature.csv"
        
    if csv_read == True:
        #从旧文件中读取
        category_feature_data = pd.read_csv(category_feature_file)
    else:
        #线上预测
        category_feature_data = item_category_pro(input_data)
        if csv_write == True:
            category_feature.to_csv(category_feature_file, index=False)

    factor_feature_data = pd.get_dummies(input_data[factor_data], columns=factor_data)
    numeric_feature_data = input_data[numeric_data]
    feature_data = pd.concat([factor_feature_data, numeric_feature_data, category_feature_data], axis=1)
    return feature_data

def do_xgb_cv(params, train_feature_data):
    
    train_x = train_feature_data.ix[train_feature_data[timestamp] < last_day_timestamp, feature_begin:]
    test_x = train_feature_data.ix[train_feature_data[timestamp] >= last_day_timestamp, feature_begin:]
    train_y = train_feature_data.loc[train_feature_data[timestamp] < last_day_timestamp, ["is_trade"]]
    test_y = train_feature_data.loc[train_feature_data[timestamp] >= last_day_timestamp, ["is_trade"]]

    #生成xgboost可以识别的训练数据和验证数据
    dtrain = xgb.DMatrix(data=train_x, label=train_y)
    xgb_cv_result = xgb.cv(params=params, dtrain=dtrain, num_boost_round=1000, nfold = 5, metrics='logloss', early_stopping_rounds=50)
    print(xgb_cv_result)
    print(xgb_cv_result.shape[0])




train_feature_data = get_feature(train_data, data_type="train", csv_read=True, csv_write=False)
train_feature_data = pd.concat([train_data[[timestamp,"is_trade", "shop_id"]],train_feature_data], axis=1)
#商店特征需要严格遵守训练集的时间窗口，不能使用测试集的数据，否则容易导致过拟合
shop_data = train_data.ix[train_data.context_timestamp < last_day_timestamp, :]
shop_feature = get_shop_feature(shop_data)
train_feature_data = pd.merge(left=train_feature_data, right=shop_feature, how="left", on="shop_id")

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
#xgb_bst = xgb_build(params, train_feature_data, num_boost_round=95)


#线上预测
test_feature_data = get_feature(test_data, data_type="test", csv_read=True, csv_write=False)
test_feature_data = pd.concat([test_data[["shop_id"]],test_feature_data], axis=1)
test_shop_data = train_data.ix[train_data.context_timestamp > second_day_timestamp, :]
test_shop_feature = get_shop_feature(test_shop_data)
test_feature_data = pd.merge(left=test_feature_data, right=test_shop_feature, how="left", on="shop_id")
test_feature_data = test_feature_data.iloc[:, 1:]
test_feature_data = get_feature(test_data, data_type="test", csv_read=True, csv_write=False)

#使用训练模型进行预测
#online_pred(xgb_bst, test_feature_data, result_file="result_test.csv")
#重新建模预测
online_rebuild_pred(train_feature_data, test_feature_data, params=params, num_boost_round=95, result_file="result_v30_0412.csv")