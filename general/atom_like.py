#模拟atom进行数据预处理

import pandas as pd
pd.set_option('display.max_rows',None)
#train_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v1/input/sinjor_train_data_rand.csv",dtype = "str")
#train_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3_used_all/input/sinjor_train_data_rand_v3_used_all.csv",dtype = "str")
train_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3.csv",dtype = "str")
#test_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3.csv",dtype = "str")
#print(train_data.columns[3:-1])
#print(train_data.label.groupby(train_data.label).count())
#获取使用的变量列名
train_data_used = train_data.columns[3:-1]
#print(train_data_used)
#对使用的变量列名进行去重处理，保留第一次找到的值
train_data.drop_duplicates(train_data_used,keep = "first", inplace = True)
#print(train_data.count())
#print(train_data.iloc[:,-2:-1].drop_duplicates().count())
#print(train_data.columns.size)
#def func(x):
#	if x.drop_duplicates().count() !=0:
#		return x
#train_data_1 = train_data.apply(func)
#删除相同元素的列（不包括NA）
train_data = train_data.ix[:, (train_data != train_data.ix[0]).any()]
#删除列值全部为NA的列
train_data.dropna(axis = 1, how = "all", inplace = True)
print(train_data.columns.size)
print(train_data.label.groupby(train_data.label).count())
#print(train_data[["event_fptokenid","event_fingerprint"]].count())
print(train_data.login_count.groupby(train_data.login_count).count())
#print(train_data.iloc[:,3:-1].head(1).) 
print(train_data.apply(lambda x: x.unique().size))