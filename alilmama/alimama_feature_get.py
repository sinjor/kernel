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

#查看训练集和测试集中user_id等数据交集记录
user_id_info = 1 *test_data.user_id.apply(lambda x: x in train_data.user_id.values)
shop_id_info = 1 *test_data.shop_id.apply(lambda x: x in train_data.shop_id.values)
context_id_info = 1 *test_data.context_id.apply(lambda x: x in train_data.context_id.values)
item_id_info = 1 *test_data.item_id.apply(lambda x: x in train_data.item_id.values)
print(user_id_info.value_counts())
print(shop_id_info.value_counts())
print(context_id_info.value_counts())
print(item_id_info.value_counts())

#唯一个数和缺失个数
def get_summary_data(input_data, miss_symbol):
    input_data.replace(miss_symbol, np.NaN, inplace=True)
    distinct_count = input_data.apply(lambda x: x.unique().size)
    dismiss_count = input_data.apply(lambda x: x.count())
    distinct_count.rename("distinct_count", inplace=True)
    dismiss_count.rename("dismiss_count", inplace=True)
    summary_data = pd.concat([distinct_count, dismiss_count],axis=1)
    summary_data.insert(loc=0, column="row_index",value=range(summary_data.index.size))
    summary_data["miss_count"] = input_data.index.size - summary_data.dismiss_count
    summary_data["variable_type"] = input_data.dtypes
    summary_data["min_value"] = input_data.min()
    summary_data["max_value"] = input_data.max()
    #summary_data["row_index"] = range(summary_data.index.size)
    #不能先重建索引，否则后面就无法根据索引新增数据了
    #summary_data.reset_index(inplace=True)
    #summary_data.rename(columns={"index":"column_name"}, inplace=True)
    return summary_data

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
	category_data = input_data
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

################################商店特征######################################
#shop_data = train_data.ix[train_data.context_timestamp < last_day_timestamp, :]
# 店铺的所有广告个数(用户个数等应该一致，前提是没用缺失值)
# 店铺的交易成功次数
# 店铺交易成功率
# 店铺的不同广告个数
# 店铺中交易成功的不同广告个数
# 店铺中交易成功的不同广告个数/店铺的不同广告个数
# 店铺的不同用户个数
# 店铺中交易成功的不同用户个数
# 店铺中交易成功的不同用户个数/店铺的不同用户个数
# 店铺的不同广告品牌编号
# 店铺中交易成功的不同广告品牌编号
# 店铺中交易成功的不同广告品牌编号/店铺的不同广告品牌编号

#unique比ddrop_duplicates运行快
#apply前面只能是一个数据列
#agg前面可以是多个数据列
#商店特征需要严格遵守训练集的时间窗口，不能使用测试集的数据，否则容易导致过拟合
def get_shop_feature(input_data):
    shop_data = input_data
    shop_id = shop_data.loc[:,["shop_id"]].drop_duplicates()
    shop_data_is_trade = shop_data.loc[shop_data["is_trade"] == 1, :]
    t1_group = shop_data.loc[:,["shop_id", "item_id", "user_id", "item_brand_id"]].groupby("shop_id")
    t1_1 = t1_group[["item_id"]].count()
    t1_1.rename(columns={"item_id":"shop_item_count"},inplace=True)
    t1_1["shop_item_dist_count"] = t1_group["item_id"].agg(lambda x:x.unique().size)
    t1_1["shop_user_dist_count"] = t1_group["user_id"].agg(lambda x:x.unique().size)
    t1_1["shop_item_brand_dist_count"] = t1_group["item_brand_id"].agg(lambda x:x[x!=-1].unique().size)
    t1_2_group = shop_data_is_trade.loc[:,["shop_id", "item_id","user_id", "item_brand_id"]].groupby("shop_id")
    t1_2 = t1_2_group[["item_id"]].count()
    t1_2.rename(columns={"item_id":"shop_y1_count"},inplace=True)
    t1_2["shop_y1_item_dist_count"] = t1_2_group["item_id"].agg(lambda x:x.unique().size)
    t1_2["shop_y1_user_dist_count"] = t1_2_group["user_id"].agg(lambda x:x.unique().size)
    t1_2["shop_y1_item_brand_dist_count"] = t1_group["item_brand_id"].agg(lambda x:x[x!=-1].unique().size)
    #t1_1 = pd.merge(left=shop_id, right=t1_1, how="left", on="shop_id")
    t1_1.reset_index(inplace=True)
    t1_2.reset_index(inplace=True)
    t1_tmp = pd.merge(left=t1_1, right=t1_2, how="left", on="shop_id")
    #t1_tmp.fillna(value=0, inplace=True)
    t1_tmp["shop_y1_item_count_div_item_count"] = t1_tmp["shop_y1_count"] / t1_tmp["shop_item_count"]
    t1_tmp["shop_y1_item_dist_count_div_item_dist_count"] = t1_tmp["shop_y1_item_dist_count"] / t1_tmp["shop_item_dist_count"]
    t1_tmp["shop_y1_user_dist_count_div_user_dist_count"] = t1_tmp["shop_y1_user_dist_count"] / t1_tmp["shop_user_dist_count"]
    t1_tmp["shop_y1_brand_dist_count_div_item_brand_dist_count"] = t1_tmp["shop_y1_item_brand_dist_count"] / t1_tmp["shop_item_brand_dist_count"]
    t1_tmp.fillna(value=0, inplace=True)
    t1 = t1_tmp
    #display(t1_tmp)

    #店铺中广告商品的价格等级,销量等级，被收藏次数的等级，展示次数的等级的平均值
    #店铺中交易成功的广告商品的价格等级,销量等级，被收藏次数的等级，展示次数的等级的平均值
    shop_id = shop_data.loc[:,["shop_id"]].drop_duplicates()
    #shop_data_is_trade = shop_data.loc[shop_data["is_trade"] == 1, :]
    tmp_data = shop_data.loc[shop_data["item_price_level"] !=-1, ["shop_id", "item_id", "item_price_level"]].drop_duplicates()
    t2_1 = tmp_data.groupby("shop_id")[["item_price_level"]].mean().reset_index()
    t2_1.rename(columns={"item_price_level":"shop_item_price_level_mean"}, inplace=True)
    tmp_data = shop_data.loc[shop_data["item_sales_level"] !=-1, ["shop_id", "item_id", "item_sales_level"]].drop_duplicates()
    t2_2 = tmp_data.groupby("shop_id")[["item_sales_level"]].mean().reset_index()
    t2_2.rename(columns={"item_sales_level":"shop_item_sales_level_mean"}, inplace=True)
    tmp_data = shop_data.loc[shop_data["item_collected_level"] !=-1, ["shop_id", "item_id", "item_collected_level"]].drop_duplicates()
    t2_3 = tmp_data.groupby("shop_id")[["item_collected_level"]].mean().reset_index()
    t2_3.rename(columns={"item_collected_level":"shop_item_collected_level_mean"}, inplace=True)
    tmp_data = shop_data.loc[shop_data["item_collected_level"] !=-1, ["shop_id", "item_id", "item_pv_level"]].drop_duplicates()
    t2_4 = tmp_data.groupby("shop_id")[["item_pv_level"]].mean().reset_index()
    t2_4.rename(columns={"item_pv_level":"shop_item_pv_level_mean"}, inplace=True)

    tmp_data = shop_data_is_trade.loc[shop_data_is_trade["item_price_level"] !=-1, ["shop_id", "item_id", "item_price_level"]].drop_duplicates()
    t2_5 = tmp_data.groupby("shop_id")[["item_price_level"]].mean().reset_index()
    t2_5.rename(columns={"item_price_level":"shop_y1_item_price_level_mean"}, inplace=True)
    tmp_data = shop_data_is_trade.loc[shop_data_is_trade["item_sales_level"] !=-1, ["shop_id", "item_id", "item_sales_level"]].drop_duplicates()
    t2_6 = tmp_data.groupby("shop_id")[["item_sales_level"]].mean().reset_index()
    t2_6.rename(columns={"item_sales_level":"shop_y1_item_sales_level_mean"}, inplace=True)
    tmp_data = shop_data_is_trade.loc[shop_data_is_trade["item_collected_level"] !=-1, ["shop_id", "item_id", "item_collected_level"]].drop_duplicates()
    t2_7 = tmp_data.groupby("shop_id")[["item_collected_level"]].mean().reset_index()
    t2_7.rename(columns={"item_collected_level":"shop_y1_item_collected_level_mean"}, inplace=True)
    tmp_data = shop_data_is_trade.loc[shop_data_is_trade["item_pv_level"] !=-1, ["shop_id", "item_id", "item_pv_level"]].drop_duplicates()
    t2_8 = tmp_data.groupby("shop_id")[["item_pv_level"]].mean().reset_index()
    t2_8.rename(columns={"item_pv_level":"shop_y1_item_pv_level_mean"}, inplace=True)
    t2_tmp = pd.merge(left=shop_id, right=t2_1, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_2, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_3, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_4, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_5, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_6, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_7, how="left", on="shop_id")
    t2_tmp = pd.merge(left=t2_tmp, right=t2_8, how="left", on="shop_id")
    t2_tmp.fillna(value=0, inplace=True)
    t2 = t2_tmp
    shop_feature = pd.merge(left=t1, right=t2, how="left", on="shop_id")
    return shop_feature
################################商店特征######################################

train_category_feature = item_category_pro(train_data)
#train_category_feature.to_csv("train_category_feature.csv", index=False)

test_category_feature = item_category_pro(test_data)
#test_category_feature.to_csv("test_category_feature.csv", index=False)
