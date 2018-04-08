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

last_day_begin_time = "2018-09-24 00:00:00"
time_array = time.strptime(last_day_begin_time, "%Y-%m-%d %H:%M:%S")
last_day_timestamp = int(time.mktime(time_array))
#print(last_day_timestamp)


################################广告类目属性特征######################################
#商品真实类目出现在预测集中的第几个
#商品真实类目出现在预测集的排序/预测集所有个数
#真实类目下属性个数和预测类目下属性个数的交集个数
#真实类目下属性个数和预测类目下属性个数的交集个数/真实类目下属性个数
#真实类目下属性个数和预测类目下属性个数的交集个数/预测类目下属性个数
#属性为-1的类目存在的个数
#属性为-1的类目存在的个数/预测集的类目个数
#商品真实类目出现在预测集中的第几个（去除-1）
#商品真实类目出现在预测集中的第几个（去除-1）/预测集所有个数

#获取预测类目列表
def get_pred_category_list(x):
    #print(x)
    category_list = list()
    for categeory in x:
        #print(string.split(":")[0])
        category_list.append(categeory.split(":")[0])
    #print(ret_list)
    return category_list
#获取真实类目在预测类目列表中的索引排序
def get_pred_category_index(x):
    #print(x["category"])
    #print(x["pred_cageg"])
    if x["item_category"] in x["pred_category_list"]:
        #索引/数量 索引不为0 因此将索引+1 同时融入了没有找到该索引的信息
        return x["pred_category_list"].index(x["item_category"]) + 1
    else:
        return 0

def get_true_category_property_in_pred_list(x):
    if x["category_index_in_pred_category_list"] == 0:
        return "NULL" #返回"NULL"
    else:
        return x["pred_category_property_list"][x["category_index_in_pred_category_list"] - 1]

def get_all_property_list(x):
    #print(x)
    property_list = list()
    for property_str in x:
        #print(string.split(":")[0])
        property_list.append(property_str.split(":")[-1])
    #print(ret_list)
    return property_list

def get_pred_category_which_has_pro_list(x):
    #print(x)
    category_list = list()
    for category in x:
        #print(string.split(":")[0])
        category_pro = category.split(":")
        if category_pro[-1] != "-1":
            category_list.append(category_pro[0])
    return category_list

def get_pred_category_which_has_pro_index(x):
    #print(x["category"])
    #print(x["pred_cageg"])
    if x["item_category"] in x["pred_category_which_has_pro_list"]:
        #索引/数量 索引不为0 因此将索引+1 同时融入了没有找到该索引的信息
        return x["pred_category_which_has_pro_list"].index(x["item_category"]) + 1
    else:
        return 0
def item_category_pro(input_data):
	category_data = train_data
	category_feature = category_data[["item_category_list", "predict_category_property","item_property_list"]]
	#item_category:当前样本所属的真实广告类目
	#pred_category_property_list:当前样本上下文预测得到的类目属性列表
	#pred_category_list:当前样本上下文预测得到的类目列表
	#category_index_in_pred_category_list: 当前样本真实类目在预测得到的类目列表中的排序索引
	#pred_category_num: 当前样本上下文预测得到的类目个数
	#category_index_div_pred_num: pred_category_num/category_index_in_pred_category_list
	category_feature["item_category"] = category_feature["item_category_list"].apply(lambda x: x.split(";")[-1])
	category_feature["pred_category_property_list"] = category_feature["predict_category_property"].apply(lambda x: x.split(";"))
	category_feature["pred_category_list"] = category_feature["pred_category_property_list"].apply(get_pred_category_list)
	category_feature["category_index_in_pred_category_list"] = category_feature.apply(get_pred_category_index, axis=1)
	category_feature["pred_category_num"] = category_feature.apply(lambda x: len(x["pred_category_list"]), axis=1)
	category_feature["category_index_div_pred_num"] = category_feature["category_index_in_pred_category_list"] / category_feature["pred_category_num"]


	#item_property_list: 当前样本的广告在某个类目下的真实属性列表
	#item_property_num: 当前样本的广告在某个类目下的真实属性个数
	#true_category_property_in_pred_list 当前样本的上下文预测得到的广告类目和真实广告类目相符的那条信息
	#pred_property_list：当前样本的上下文预测得到的广告类目与真实广告类目相符的那条信息的属性列表
	#pred_property_num：当前样本的上下文预测得到的广告类目与真实广告类目相符的那条信息的属性个数
	#pred_pro_intersection_item_pro_num：当前样本的上下文预测得到的广告类目与真实广告类目相符的那条信息的属性个数和真实属性的交集

	category_feature["item_property_list"] = category_feature.item_property_list.apply(lambda x:x.split(";"))
	#真实广告属于同一个类别，但会拥有不同的属性
	category_feature["item_property_num"] = category_feature.item_property_list.apply(lambda x:len(x))    
	#上下文预测中和广告类别预测一致的信息
	category_feature["true_category_property_in_pred_list"] = category_feature.apply(get_true_category_property_in_pred_list, axis=1)
	category_feature["pred_property_list"] = category_feature["true_category_property_in_pred_list"].apply(lambda x:x.split(":")[-1].split(","))
	category_feature["pred_property_num"] = category_feature["pred_property_list"].apply(lambda x:len(x))
	#set的结果是元组
	category_feature["pred_pro_intersection_item_pro_num"] = category_feature.apply(lambda x: len(set(x["pred_property_list"]).intersection(set(x["item_property_list"]))), axis=1)


	#pred_pro_intersection_item_pro_num_div_pred_property_num  pred_pro_intersection_item_pro_num/pred_property_num
	#pred_pro_intersection_item_pro_num_div_item_property_num  pred_pro_intersection_item_pro_num/item_property_num
	#pred_all_property_list 
	#pred_category_which_property_is_
	#pred_category_which_property_is_
	category_feature["pred_pro_intersection_item_pro_num_div_pred_property_num"] = category_feature.apply(lambda x:x["pred_pro_intersection_item_pro_num"]/x["pred_property_num"],axis=1)
	category_feature["pred_pro_intersection_item_pro_num_div_item_property_num"] = category_feature.apply(lambda x:x["pred_pro_intersection_item_pro_num"]/x["item_property_num"],axis=1)
	category_feature["pred_all_property_list"] = category_feature.pred_category_property_list.apply(get_all_property_list)
	category_feature["pred_category_which_property_is_-1_num"] = category_feature["pred_all_property_list"].apply(lambda x:x.count('-1'))
	category_feature["pred_category_which_property_is_-1_num_div_pred_category_num"] = category_feature.apply(lambda x:x["pred_category_which_property_is_-1_num"]/x["pred_category_num"], axis=1)

	category_feature["pred_category_which_has_pro_list"] =  category_feature.pred_category_property_list.apply(get_pred_category_which_has_pro_list)
	category_feature["pred_category_which_has_pro_num"] = category_feature.apply(get_pred_category_which_has_pro_index, axis=1)
	category_feature["pred_category_which_has_pro_num_div_pred_category_num"] = category_feature.apply(lambda x: x["pred_category_which_has_pro_num"]/x["pred_category_num"], axis=1)

	train_category_feature = category_feature[["category_index_in_pred_category_list","category_index_div_pred_num","pred_pro_intersection_item_pro_num",
	    "pred_pro_intersection_item_pro_num_div_pred_property_num","pred_pro_intersection_item_pro_num_div_item_property_num",
	                "pred_category_which_property_is_-1_num","pred_category_which_property_is_-1_num_div_pred_category_num",
	                "pred_category_which_has_pro_num","pred_category_which_has_pro_num_div_pred_category_num"]]
	return train_category_feature
################################广告类目属性特征######################################
#
#train_category_feature = item_category_pro(train_data)
#train_category_feature.to_csv("train_category_feature.csv", index=False)
#从旧文件中读取
train_category_feature = pd.read_csv("train_category_feature.csv")

factor_feature_data = pd.get_dummies(train_data[factor_data], columns=factor_data)
numeric_feature_data = train_data[numeric_data]

#feature_data = pd.concat([factor_feature_data, numeric_feature_data], axis=1)
#data  = feature_data
#label = train_data.is_trade
#随机将数据集划分成训练集和验证集
#train_x, test_x, train_y, test_y = train_test_split(data, label, train_size = 0.7, random_state=0)

feature_data = pd.concat([train_data[[timestamp,"is_trade"]],factor_feature_data, numeric_feature_data, train_category_feature], axis=1)
#前6天作为训练集，最后一天作为验证集
train_x = feature_data.ix[feature_data[timestamp] < last_day_timestamp, 2:]
test_x = feature_data.ix[feature_data[timestamp] >= last_day_timestamp, 2:]
train_y = feature_data.loc[feature_data[timestamp] < last_day_timestamp, ["is_trade"]]
test_y = feature_data.loc[feature_data[timestamp] >= last_day_timestamp, ["is_trade"]]

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
bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=121, evals=watchlist)
#bst = xgb.train(params=params, dtrain=dtrain, num_boost_round=103)
test_pred = bst.predict(data=dtest)
logloss_prob = metrics.log_loss(y_true=test_y, y_pred=test_pred)
print("logloss_prob:",logloss_prob)

#线上预测
#test_category_feature = item_category_pro(test_data)
#test_category_feature.to_csv("test_category_feature.csv", index=False)
##从旧文件中读取
test_category_feature = pd.read_csv("test_category_feature.csv")

factor_feature_data = pd.get_dummies(test_data[factor_data], columns=factor_data)
numeric_feature_data = test_data[numeric_data]
test_feature_data = pd.concat([factor_feature_data, numeric_feature_data, test_category_feature], axis=1)

dtest = xgb.DMatrix(data=test_feature_data) 
test_pred = bst.predict(data=dtest)

result = test_data[["instance_id"]]
#print(type(result))
result["predicted_score"] = test_pred
print(result.head())
result.to_csv("result_v20.csv", sep=" ", index=False, line_terminator='\n')