#输入算法的数据分析
import pandas as pd 
import matplotlib.pyplot as plt
#pd.set_option('display.max_rows',None)

#variable_weight = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/GBM/Output/learn/Model_VariableWeights.csv")
input_train_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3.csv", dtype = "str")
input_test_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3.csv", dtype = "str")
#test_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3.csv",dtype = "str")
#dict_data = pd.read_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/xh_dict.csv",dtype = "str")

#print(train_data.event_mobilecity_code.groupby(train_data.label).agg(lambda x:x.count()))
#print(train_data[["event_mobilecity_code", "label"]].groupby(train_data.event_mobilecity_code).agg(lambda x:x.count()))

train_data = input_train_data.loc[:, ["event_mobilecity_code","label"]].astype(int)
#train_data.label = train_data.label.astype(int)
input_train_data.loc[:, ["event_mobilecity_code","label"]] = input_train_data.loc[:, ["event_mobilecity_code","label"]].astype(int)
input_test_data.loc[:, ["event_mobilecity_code","label"]] = input_test_data.loc[:, ["event_mobilecity_code","label"]].astype(int)

#print(train_data.info())
event_mobilecity_code_group = train_data[["event_mobilecity_code", "label"]].groupby(["event_mobilecity_code", "label"])["label"].count()
event_mobilecity_code_summary = event_mobilecity_code_group.loc[:,0].to_frame(name = "label_0")
event_mobilecity_code_summary = pd.concat([event_mobilecity_code_summary,event_mobilecity_code_group.loc[:,1].rename("label_1")], axis = 1, join = "outer")
#event_mobilecity_code_summary["label_1"] = event_mobilecity_code_group.loc[:,1] #不能直接赋值，event_mobilecity_code_summary的索引会缺失
event_mobilecity_code_summary.fillna(value = 1, inplace = True)
event_mobilecity_code_summary = event_mobilecity_code_summary.astype("int64")
event_mobilecity_code_summary["label_0_1_sum"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_0"] + x.loc["label_1"], axis = 1)
event_mobilecity_code_summary["proportion"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_0_1_sum"] /69999, axis = 1)
event_mobilecity_code_summary["label_1_div_all"] = event_mobilecity_code_summary.apply(lambda x: x.loc["label_1"] / x.loc["label_0_1_sum"], axis = 1)
event_mobilecity_code_summary["label_1_div_data"] = event_mobilecity_code_summary.apply(lambda x: (x.loc["proportion"] * x.loc["label_1_div_all"]), axis = 1)

#event_mobilecity_code_summary.sort_values(by = "label_1", inplace = True)
#event_mobilecity_code_summary.reset_index(inplace = True)
#print(event_mobilecity_code_summary.label_1.head())
#event_mobilecity_code_summary[["label_1"]].plot(kind = "line")
#plt.show()

#event_mobilecity_code_summary.sort_values(by = "label_1_div_all", inplace = True)
#event_mobilecity_code_summary.reset_index(inplace = True)
#event_mobilecity_code_summary[["label_1_div_all"]].plot(kind = "line")
#plt.show()


#event_mobilecity_code_summary["sort_label_1_id"] = event_mobilecity_code_summary.label_1.rank(ascending = False, method = "min")
#event_mobilecity_code_summary["sort_label_1_div_all_id"] = event_mobilecity_code_summary.label_1_div_all.rank(ascending = False, method = "min")
def label_1_flag_func (x):
	if (x.loc["label_1"] < 10) & (x.loc["label_1_div_all"] < 0.05) :
		return 0
	elif (x.loc["label_1"] < 10) & (x.loc["label_1_div_all"] >= 0.05) :
		return 1
	elif (x.loc["label_1"] >= 10) & (x.loc["label_1_div_all"] < 0.05) :
		return 2
	elif (x.loc["label_1"] >= 10) & (x.loc["label_1_div_all"] >= 0.05) :
		return 3
	else:
		return 4
print(event_mobilecity_code_summary.info())
event_mobilecity_code_summary["event_mobilecity_code_4_flag"] = event_mobilecity_code_summary.apply(label_1_flag_func, axis = 1)
print(event_mobilecity_code_summary.event_mobilecity_code_4_flag.value_counts())
#event_mobilecity_code_summary.sort_values("label_0_1_sum", ascending = False, inplace = True)
#
#event_mobilecity_code_summary.reset_index(inplace = True)
#event_mobilecity_code_summary["sort_id"] = event_mobilecity_code_summary.label_0_1_sum.rank(ascending = False, method = "min")
#event_mobilecity_code_summary["event_mobilecity_code_sum_flag"] = event_mobilecity_code_summary.apply(lambda x: int(x.loc["sort_id"]/16), axis =1)
#event_mobilecity_code_summary.loc[event_mobilecity_code_summary.loc[:,"event_mobilecity_code_sum_flag"]>5, "event_mobilecity_code_sum_flag"] = 5
#def label_flag_func (x):
#	if x.loc["sort_id"] < 80:
#		return int(x.loc["label_prop_sort_id"]/16)
#	else:
#		return 5
#
#event_mobilecity_code_summary["label_prop_sort_id"] = event_mobilecity_code_summary.label_1_div_data[event_mobilecity_code_summary.sort_id <80].rank(ascending = False, method = "min")
#event_mobilecity_code_summary["event_mobilecity_code_label_flag"] = event_mobilecity_code_summary.apply(label_flag_func, axis =1)
print(event_mobilecity_code_summary.info())
event_mobilecity_code_summary.reset_index(inplace = True)
event_mobilecity_code_feture = event_mobilecity_code_summary[["event_mobilecity_code", "event_mobilecity_code_4_flag"]]


#print(event_mobilecity_code_feture.event_mobilecity_code_label_flag.head())
input_train_data = pd.merge(input_train_data, event_mobilecity_code_feture, how = "left", on = "event_mobilecity_code")
input_test_data = pd.merge(input_test_data, event_mobilecity_code_feture, how = "left", on = "event_mobilecity_code")
input_train_data.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3_b3.csv",na_rep = "NULL")
input_test_data.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3_b3.csv",na_rep = "NULL")

#print(input_train_data.event_mobilecity_code_label_flag.unique())
#event_mobilecity_code_summary.loc[event_mobilecity_code_summary.loc[:,"event_mobilecity_code_sum_flag"]>5, "event_mobilecity_code_sum_flag"] = 5
#print(event_mobilecity_code_summary.head())
#print(event_mobilecity_code_summary.event_mobilecity_code_label_flag.unique())
#event_mobilecity_code_summary.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/event_mobilecity_code_summary2.csv")
#print(event_mobilecity_code_summary[event_mobilecity_code_summary.label_0 == 0])

'''
event_mobilecity_code_label_1 = train_data.event_mobilecity_code[train_data.label == 1].groupby(train_data.event_mobilecity_code).count().sort_index()
#print(train_data.event_mobilecity_code[train_data.label == "1"].value_counts().sort_values())
event_mobilecity_code_label_0 = train_data.event_mobilecity_code[train_data.label == 0].groupby(train_data.event_mobilecity_code).count().sort_index()
event_mobilecity_code_label_1 = event_mobilecity_code_label_1.rename("count_label_1")
#event_mobilecity_code_label_1.index.astype("int")
event_mobilecity_code_label_0 = event_mobilecity_code_label_0.rename("count_label_0")
#event_mobilecity_code_label_0.index.astype("int")
event_mobilecity_code_label = pd.concat([event_mobilecity_code_label_0,event_mobilecity_code_label_1], axis = 1, join = "outer")
event_mobilecity_code_label["proportion"] = train_data.event_mobilecity_code.groupby(train_data.event_mobilecity_code).count()/train_data.index.size
#print(event_mobilecity_code_label[event_mobilecity_code_label.event_mobilecity_code_label_1.isnull()])
#print(event_mobilecity_code_label[event_mobilecity_code_label.event_mobilecity_code_label_0.isnull()])
print(event_mobilecity_code_label[(event_mobilecity_code_label.index > 229) & (event_mobilecity_code_label.index < 243)])
print(train_data.index.size)
'''
#cid_apply_1m_hf_label = train_data[["cid_apply_1m_hf", "label", "cid"]].groupby(["cid_apply_1m_hf", "label"]).count()

#print(cid_apply_1m_hf_label.sort_values("cid"))
#print(train_data.head(5))

'''
#"event_contactsrelationship_code.13",2.14--->event_contactsrelationship_code 
#根据.分割字符串，并获取第一个字符串
variable_weight.variable = variable_weight.variable.apply(lambda x: x.split(".")[0])
variable_weight.drop_duplicates("variable" ,inplace = True)
#去除权重为0的变量
variable_weight = variable_weight[variable_weight.weights > 0]
#获取NumberOfMissingValues所在的行号，并用drop删除该行
drop_row_index = variable_weight[variable_weight.variable.isin(["NumberOfMissingValues"])].index.values
variable_weight.drop(drop_row_index, inplace = True)
#重新排列索引，并删掉旧索引
variable_weight.reset_index(drop = True, inplace = True)
#series转换成list
variable_used_columns_header = variable_weight.variable.tolist()
#在list首部插入两个值
variable_used_columns_header.insert(0,"label")
variable_used_columns_header.insert(0,"cid")
#使用list建立dataframe，列名为需要和dict_data中的变量列名一致，便于merge
dict_variable_used_columns =pd.DataFrame( variable_used_columns_header, columns = ["variable"])

#dict_variable_used_columns.join(dict_data, on = "variable", how = "left")
#这里需要用merge，去除dict_data中其他变量
dict_variable_used_columns = pd.merge(dict_variable_used_columns, dict_data, how = "left", on = "variable")

train_variable_used_columns = train_data[variable_used_columns_header]
test_variable_used_columns = test_data[variable_used_columns_header]
train_variable_used_columns.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_train_data_rand_v3_used.csv",index = False, na_rep = "NULL")
test_variable_used_columns.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/sinjor_test_data_rand_v3_used.csv",index = False, na_rep = "NULL")
dict_variable_used_columns.to_csv("G:/taodata/project/xiaohua_finance/data_0116_sinjor_rand_v3/input/xh_dict_used.csv",index = False, na_rep = "NULL")
'''

exit()