#处理部分o2o的特征
import numpy as np
import pandas as pd
from autoplot import AutoPlot
from datetime import date
'''
RangeIndex: 1754884 entries, 0 to 1754883
Data columns (total 7 columns):
User_id          int64
Merchant_id      int64
Coupon_id        object
Discount_rate    object
Distance         object
Date_received    object
Date             object
dtypes: int64(2), object(5)


In [44]: test_data.info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 113640 entries, 0 to 113639
Data columns (total 6 columns):
User_id          113640 non-null int64
Merchant_id      113640 non-null int64
Coupon_id        113640 non-null int64
Discount_rate    113640 non-null object
Distance         113640 non-null object
Date_received    113640 non-null int64
dtypes: int64(4), object(2)
memory usage: 5.2+ MB

train_data:
              User_id  Merchant_id  Coupon_id  Discount_rate  Distance  Date_received  Date            
value_counts   539438         8415       9739             46        12            168   183  

test_data:
              User_id  Merchant_id  Coupon_id  Discount_rate  Distance  Date_received  
value_counts    76309         1559       2050             42        12             31 

train_test_data:

Coupon_id         11789
Date                183
Date_received       199
Discount_rate        47
Distance             12
Merchant_id        8416
User_id          539440
dtype: int64
         
         train_on27748
'''

#auto_plot = AutoPlot("Target","G:/taodata/Atom/GermanCredit/train.csv", debug = True)
#auto_plot.plt_continuous_kde(auto_plot.train_data[["Age", "Target"]], "Target","Age")
print("hello test")
train_data = pd.read_csv("G:/program_monkey/ML/tianchi/O2O/ccf_offline_stage1_train_1.csv")
#test_data = pd.read_csv("G:/program_monkey/ML/tianchi/O2O/ccf_offline_stage1_test_revised.csv")
#train_on_data = pd.read_csv("G:/program_monkey/ML/tianchi/O2O/ccf_online_stage1_train.csv")
#
#train_data_1.to_csv("G:/program_monkey/ML/tianchi/O2O/ccf_offline_stage1_train_1.csv")
#train_data_1 = train_data[(train_data.Date < "20160414") | (train_data.Date_received < "20160330")]

def label_extract(date_label):
	'''
	提取label
	'''	
	date_label = date_label.split(":")
	#Date_received == "null" 普通购物
	if date_label[0] =="null":
		return 0
	#Date=null & Date_received != null，该记录表示领取优惠券但没有使用，即负样本；
	if (date_label[1] == "null") & (date_label[0] != "null"):
		return -1

	date_day = date(int(date_label[1][0:4]), int(date_label[1][4:6]), int(date_label[1][6:8])) - date(int(date_label[0][0:4]), int(date_label[0][4:6]), int(date_label[0][6:8]))
	if date_day.days <=15:
		return 1
	else:
		return -1

train_data["label"] = train_data.Date_received + ":" + train_data.Date
train_data.label = train_data.label.apply(label_extract)
#print(train_data.label)
#获取仅有某列series的DataFrame
user_id_feature = train_data[["User_id"]]
#数据框去重，返回仍然是数据框，因此使用此方法获取key = user_id的数据框
user_id_feature = user_id_feature.drop_duplicates()

#数据框转换成Series，再转换成数据框，获取列中元素个数 index为User_id
#feature1:用户15日内核销优惠劵的次数
feature1 = train_data.User_id[train_data.label == 1].value_counts()
feature1 = feature1.to_frame(name = "label_1_count")
#将新的数据框与基数据框根据User_id合并
user_id_feature = user_id_feature.join(feature1, how = "left", on = "User_id")
#合并后由于两个数据框的key不一致，会出现空值，将空值用0填充
user_id_feature = user_id_feature.fillna(0)
#print(user_id_feature)
#
#用户线下相关的特征
'''
用户领取优惠券次数                                                 Date_received
用户获得优惠券但没有消费的次数                                     Date_received Date
用户获得优惠券并核销次数                                           Date_received Date
用户领取优惠券后进行核销率                                         Date_received Date
用户满0~50/50~200/200~500 减的优惠券核销率                         Discount_rate Date
用户核销满0~50/50~200/200~500减的优惠券占所有核销优惠券的比重      Discount_rate Date
用户核销优惠券的平均/最低/最高消费折率                             Discount_rate Date
用户核销过优惠券的不同商家数量，及其占所有不同商家的比重           Merchant_id Date
用户核销过的不同优惠券数量，及其占所有不同优惠券的比重             Coupon_id Date
用户平均核销每个商家多少张优惠券                                   Merchant_id Date
用户核销优惠券中的平均/最大/最小用户-商家距离                      Distance Date
'''
#用户领取优惠券次数
t1 = train_data.User_id[train_data.Coupon_id != "null"]
t1["user_reveive_coupon_count"] = 0
t1 = t1.groupby("User_id").count()
#用户获得优惠券但没有消费的次数
t2 = train_data.User_id[(train_data.Coupon_id != "null") & (train_data.Date =="null")]
t2["user_receive_coupon_not_use_count"] = 0
t2 = t2.groupby("User_id").count()
#用户获得优惠券并核销次数 
t3 = train_data.User_id[(train_data.Coupon_id != "null") & (train_data.Date !="null")]
t3["user_receive_coupon_used_count"] = 0
t3 = t3.groupby("User_id").count()
#用户领取优惠券后进行核销率
#用户满0~50/50~200/200~500 减的优惠券核销率 
#
#用户核销过优惠券的不同商家数量，
t8 = train_data[["User_id", "Merchant_id"]][(train_data.Coupon_id !="null") & (train_data.Date != "null")]
#t8["user_used_dis_merchant"] = 0
t8 = t8.groupby("User_id").agg(lambda x: x.unique().size)
t8.rename(columns = {"Merchant_id" : "user_used_dis_merchant"},inplace = True)

train_data[(train_data.Coupon_id != "null") &(train_data.Date != "null")].groupby("User_id").agg(lambda x: x.unique().size).head()

train_data[(train_data.Coupon_id != "null") & (train_data.Date != "null")].groupby("User_id").count()
'''



#根据列名创建数据框 索引可以为空，也可以给初始值
feature_data = pd.DataFrame(columns = train_data.columns.values)
#loc用于新增单号索引，或者引用索引，对于列值可以用None作为默认，或者使用字典形式进行赋值
feature_data.loc["value_counts"] = None
for columns_header, columns_data in train_data.iteritems():
	feature_data[columns_header] = columns_data.value_counts().size
print(feature_data)

#以上的计算可以用如下单行代替
print(train_data.apply(lambda x: x.value_counts().size))
'''

#train_data.sort_values()
#[(A) & (B)] 括号不能少
#train_data.User_id[(train_data.Date == "null") & (train_data.Date_received =="null")].value_counts().size

#train_test_data = pd.concat([train_data, test_data])
#三种方式进行去重后数据个数统计 
#drop_duplicates().count() 
#values_count().size 
#unique().size
'''
print(train_data.apply(lambda x:x.drop_duplicates().count()))
print(test_data.apply(lambda x:x.drop_duplicates().count()))
print(train_test_data.apply(lambda x:x.drop_duplicates().count()))

print(train_data.apply(lambda x: x.value_counts().size))
print(test_data.apply(lambda x: x.value_counts().size))
print(train_test_data.apply(lambda x: x.value_counts().size))

print(train_data.apply(lambda x: x.unique().size))
'''



