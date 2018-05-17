#对数据进行概要分析，并生成表格
#dist_count ：去重后的变量个数
#non_empty_count ：非空的变量个数
#variable_type ：python识别的变量数据类型
import pandas as pd
pd.set_option('display.max_rows',None)
data_path = "G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/"
train_data_file = data_path + "train.csv"
summary_data_file = data_path + "train_data_summary.csv"
train_data = pd.read_csv(train_data_file)
#train_data_100 = train_data.iloc[:,0:100]
#train_data = train_data_100
#train_data.VAR_0008.value_counts(dropna=False)
#unique包含了空值，value_counts()不包含空值，value_counts(dropna=False)包含空值
dist_count = train_data.apply(lambda x: x.unique().size)
non_empty_count = train_data.apply(lambda x: x.count())
dist_count.rename("dist_count", inplace = True)
non_empty_count.rename("non_empty_count", inplace = True)
train_data_summary = pd.concat([dist_count, non_empty_count], axis = 1)
train_data_summary["variable_type"] = train_data.dtypes
#train_data_summary.index.rename("variable",inplace=True)
#print(train_data_summary)
train_data_summary.to_csv(summary_data_file, drop_in )
exit()

############################################唯一个数和缺失个数
def get_summary_data(data, miss_symbol):
    input_data = data.replace(miss_symbol, np.NaN)
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

    
'''
          dist_count  non_empty_count variable_type
variable                                           
ID            145231           145231         int64
VAR_0001           3           145231        object
VAR_0002         820           145231         int64
VAR_0003         588           145231         int64
VAR_0004        7935           145231         int64
VAR_0005           4           145231        object
'''