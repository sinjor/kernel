#�����ݽ��и�Ҫ�����������ɱ��
#dist_count ��ȥ�غ�ı�������
#non_empty_count ���ǿյı�������
#variable_type ��pythonʶ��ı�����������
import pandas as pd
pd.set_option('display.max_rows',None)
data_path = "G:/program_monkey/ML/kaggle/Springleaf_Marketing_Response/data/train_csv/"
train_data_file = data_path + "train.csv"
summary_data_file = data_path + "train_data_summary.csv"
train_data = pd.read_csv(train_data_file)
#train_data_100 = train_data.iloc[:,0:100]
#train_data = train_data_100
#train_data.VAR_0008.value_counts(dropna=False)
#unique�����˿�ֵ��value_counts()��������ֵ��value_counts(dropna=False)������ֵ
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