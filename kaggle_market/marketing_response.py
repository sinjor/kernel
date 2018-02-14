import pandas as pd
pd.set_option('display.max_rows',None)
train_data = pd.read_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/train.csv")
train_data_100 = train_data.iloc[:,0:100]
train_data = train_data_100

train_data_100_dtypes = train_data_100.dtypes
train_data_100_object = train_data_100_dtypes[train_data_100_dtypes.values == 'object']

dist_count = train_data.apply(lambda x: x.value_counts().size)
count = train_data.apply(lambda x: x.count())
dist_count.rename("dist_count", inplace = True)
count.rename("count", inplace = True)
train_data_summary = pd.concat([dist_count,count], axis = 1)
train_data_summary["type"] = train_data.dtypes

train_data_summary.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/train_data_summary.csv")

#随机划分训练集和测试集 sinjor_train_data_rand.csv sinjor_test_data_rand.csv
train_data = pd.read_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/train.csv", dtype='str')
#删除object数据类型的类
drop_list = train_data_summary[train_data_summary.type == 'object']
train_data.drop(labels=drop_list.index,axis=1, inplace=True)
#随机划分
train_data["rand"] = np.random.rand(train_data.index.size)
train_data.sort_values("rand",inplace=True)
train_data.reset_index(drop=True,inplace=True)
#将target列移到ID之后
target = train_data.pop("target")
train_data.insert(1, "target", target)
train_data_rand = train_data.iloc[0:101661,:]
train_data_rand.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/sinjor_train_data_rand.csv")
test_data_rand = train_data.iloc[101661:,:]
test_data_rand.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/sinjor_test_data_rand.csv")
#生成数据字典
dict_data = train_data.columns.to_series()
dict_data[:] = "numeric"
dict_data.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/sinjor_dict.csv")

#获取指定的object列

train_data_summary = pd.read_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/train_data_summary.csv")
variable_weights = pd.read_csv("G:/taodata/project/kaggle_market_response/atom7.2_0207_rand_v1/rf/Output/Learn/Model_VariableWeights.csv")
train_data_summary.rename(columns={"Unnamed: 0":"variable"},inplace=True)

#获取类别个数小于100 数据非空个数大于10000的object类型数据
variable_object = train_data_summary.loc[(train_data_summary.dist_count>1) & (train_data_summary.dist_count<100) & (train_data_summary.type == 'object') & (train_data_summary["count"] > 10000),["variable"]]

#获取模型权重大于0.25的变量
variable_weights = variable_weights.loc[variable_weights.weights > 0.25,["variable"]]

#生成ID和target列名
variable_target = pd.DataFrame(["ID","target"],columns=["variable"])

#将需要提取的变量合并
variable = pd.concat([variable_target, variable_weights, variable_object])
variable.reset_index(drop=True,inplace=True)

#随机划分训练集和测试集 sinjor_train_data_rand_v2.csv sinjor_test_data_rand_v2.csv
train_data = pd.read_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/train.csv", dtype='str')
variable_data = train_data.loc[:,variable.variable]

variable_data["rand"] = np.random.rand(variable_data.index.size)
variable_data.sort_values("rand",inplace=True)
variable_data.reset_index(drop=True,inplace=True)

#将factor中的"-1"用"1"替换，"Discharge NA"用"DischargeNA"替换,原因是atom不支持“-”和“ ”
variable_data.loc[:,variable_object.variable] = variable_data.loc[:,variable_object.variable].replace("-1","1")
variable_data.VAR_0467.replace("Discharge NA","DischargeNA",inplace=True)
train_data_rand = variable_data.iloc[0:101661,:]
train_data_rand.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/sinjor_train_data_rand_v2.csv")
test_data_rand = variable_data.iloc[101661:,:]
test_data_rand.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/sinjor_test_data_rand_v2.csv")

#生成数据字典
dict_data = variable_data.columns.to_series()
dict_data[:] = "numeric"
dict_data.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/sinjor_dict_v2.csv")


#加工测试数据
test_data = pd.read_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/test_csv/test.csv", dtype='str')
variable_target = pd.DataFrame(["ID"],columns=["variable"])
variable = pd.concat([variable_target, variable_weights, variable_object])
variable.reset_index(drop=True,inplace=True)
test_data = test_data.loc[:,variable.variable]
test_data.loc[:,variable_object.variable] = test_data.loc[:,variable_object.variable].replace("-1","1")
test_data.VAR_0467.replace("Discharge NA","DischargeNA",inplace=True)
test_data.to_csv("G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/test_csv/test_v2.csv")



dist_count[train_data_100_object.index]
'''
VAR_0001       3
VAR_0005       4
VAR_0008       1
VAR_0009       1
VAR_0010       1
VAR_0011       1
VAR_0012       1
VAR_0043       1
VAR_0044       1
VAR_0073    1458
VAR_0075    2371
'''
count[train_data_100_object.index]

'''
VAR_0001    145231
VAR_0005    145231
VAR_0008    145175
VAR_0009    145175
VAR_0010    145175
VAR_0011    145175
VAR_0012    145175
VAR_0043    145175
VAR_0044    145175
VAR_0073     44104 date
VAR_0075    145175 date
'''
'''
train_data_summary[train_data_summary.type == 'object']
          dist_count   count    type
VAR_0001           3  145231  object 未知   +1
VAR_0005           4  145231  object 未知   +1
VAR_0008           1  145175  object
VAR_0009           1  145175  object
VAR_0010           1  145175  object
VAR_0011           1  145175  object
VAR_0012           1  145175  object
VAR_0043           1  145175  object
VAR_0044           1  145175  object
VAR_0073        1458   44104  object date
VAR_0075        2371  145175  object date
VAR_0156         730    5870  object date
VAR_0157         424     920  object date
VAR_0158         407    2089  object date
VAR_0159         650    5870  object date
VAR_0166        2145   14230  object date
VAR_0167         853    2567  object date
VAR_0168        1645   10725  object date
VAR_0169        1908   14230  object date
VAR_0176        2163   17532  object date
VAR_0177         945    3358  object date
VAR_0178        1648   12073  object date
VAR_0179        1875   17532  object date
VAR_0196           1  145175  object
VAR_0200       12385  145171  object city
VAR_0202           1  145175  object
VAR_0204        1192  145175  object date_time
VAR_0214          12      12  object
VAR_0216           1  145175  object
VAR_0217         397  145175  object date
VAR_0222           1  145175  object
VAR_0226           2  145175  object boolean   +1
VAR_0229           1  145175  object
VAR_0230           2  145175  object boolean   +1
VAR_0232           2  145175  object boolean   +1
VAR_0236           2  145175  object boolean   +1
VAR_0237          45  145175  object 未知
VAR_0239           1  145175  object
VAR_0274          57  144313  object 未知 同237
VAR_0283           7  144313  object 未知             +1
VAR_0305           8  144313  object 未知同283 +M      +1
VAR_0325           9  144313  object 未知同283 +M +G      +1
VAR_0342          50  144313  object 未知
VAR_0352           4  144313  object 未知              +1
VAR_0353           4  144313  object 未知 同352        +1
VAR_0354           4  144313  object 未知 同352        +1
VAR_0404        1823  144313  object 未知
VAR_0466           2  144313  object 未知               +1
VAR_0467           4  144313  object 未知             +1
VAR_0493         608  144313  object 职业
VAR_1934           5  145231  object 未知              +1
'''