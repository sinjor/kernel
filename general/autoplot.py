# -*- coding: utf-8 -*-
"""
Created on Sun Oct 15 10:43:32 2017

@author: Administrator
"""
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import sklearn.preprocessing as preprocessing
from sklearn import linear_model

import time


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
'''
'''
print(train_data.info())

id                           800 non-null int64
Target                       800 non-null int64
AccountStatus                800 non-null object
DurationInMonth              800 non-null int64
CreditHistory                800 non-null object
Purpose                      800 non-null object
CreditAmount                 800 non-null int64
SavingsAccount               800 non-null object
Employment                   800 non-null object
InstallmentRate              800 non-null int64
PersonalStatus               800 non-null object
OtherDebtors                 800 non-null object
PresentResidenceSince        800 non-null int64
Property                     800 non-null object
Age                          800 non-null int64
OtherInstallmentPlans        800 non-null object
Housing                      800 non-null object
NumberOfCreditsInBank        800 non-null int64
Job                          800 non-null object
NumberOfPeopleMaintenance    800 non-null int64
Telephone                    800 non-null object
ForeignWorker                800 non-null object
'''
#print(train_data.describe())
#output_graph_path = "G:/taodata/Atom/GermanCredit/"
#train_data = pd.read_csv("G:/taodata/Atom/GermanCredit/train.csv")
class AutoPlot(object):
    '''
    label_header:label name 
    csv_file_path:input
    debug:default: false
    '''
    def __init__(self, label_header, csv_file_path, debug = False):
        self.label_header = label_header
        self.train_data = pd.read_csv(csv_file_path)
        #获取csv文件所在的路径，用于存放输出的图表
        self.output_graph_path = csv_file_path[0:csv_file_path.rindex("/") + 1]
        self.debug = debug



    def plt_factor_bar(self, sample, label_header, feature_header):
        '''
        离散型特征绘制直方图
        '''
        if label_header == feature_header:
            print("label_header == feature_header")
            return
        #获取lable和单列特征数据
        #sample = train_data.iloc[:, [1,2]]
        #获取Label和Feature名称，
        #'Series' object has no attribute 'columns'
        #Series 和DataFrame的区别
        #label_header = pdLabel.columns[0]
        #feature_header = pdFeature.columns[0]
        #label_header = pdLabel.name
        #feature_header = pdFeature.name
        #数据框合并
       # sample = pd.concat([pdLabel, pdFeature], axis = 1)
        
        #对特征进行因子化，类似于onehot，获取各个因子名称
        factors = sample[feature_header].value_counts()
        factor_name_list = factors.index.values.tolist()
        #根据因子名称，建立因子字典，用于创建因子数据框
        factor_dic = {}
        for factor_name in factor_name_list:
            factor_dic[factor_name] = sample[label_header][sample[feature_header] == factor_name].value_counts()
        
        #根据直方图模式选择是否转置，因子较多时进行转置，图比较清晰
        #factors = pd.DataFrame(factor_dic).transpose()
        factors = pd.DataFrame(factor_dic)
        #print(factors)
        
        factors.plot(kind = "bar", stacked = True)          #堆积柱状图
        plt.title("Feature: " + feature_header)
        plt.xlabel("Label: " + label_header)
        plt.ylabel(u"count")
        self.__plt_show(plt, feature_header)
        

    def plt_continuous_kde(self, sample, label_header, feature_header):
        '''
        连续型特征绘制密度图
        sample:DataFrame 样本表格
        Label_header:string 标签列名称
        feature_header:string 特征列名称
        '''  
        if label_header == feature_header:
            print("label_header == feature_header")
            return
        #根据标签类分离连续特征，并新增两列
        label_header_0 = label_header +"_0"
        label_header_1 = label_header +"_1"
        sample[label_header_0] = sample[feature_header][sample[label_header] == 0]
        sample[label_header_1] = sample[feature_header][sample[label_header] == 1]

        sample[[feature_header, label_header_0, label_header_1]].plot(kind="kde", grid=True)

        plt.title("Feature: " + feature_header)
        plt.xlabel(feature_header)
        plt.ylabel(u"density")
        self.__plt_show(plt, feature_header)  
        #在 plt.show() 后实际上已经创建了一个新的空白的图片（坐标轴），
        #这时候你再 plt.savefig() 就会保存这个新生成的空白图片。
        '''
        current_time = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
        graph_file_path = output_graph_path + feature_header + current_time
        plt.savefig(graph_file_path)
        plt.show()
        '''
        
    def plt_continuous_scatter(self, sample, label_header, feature_header):
        '''
        连续型特征绘制散点图
        sample:DataFrame 样本表格
        Label_header:string 标签列名称
        feature_header:string 特征列名称
        '''  
        if label_header == feature_header:
            print("label_header == feature_header")
            return
        #根据标签类分离连续特征，并新增两列
        label_header_0 = label_header + "_0"
        label_header_1 = label_header + "_1"
        sample[label_header_0] = sample[feature_header][sample[label_header] == 0]
        sample[label_header_1] = sample[feature_header][sample[label_header] == 1]
        #pandas自带的没法将两个label合并在一起，reset_index将索引加入列中，绘制scatter时需要使用index
        #sample.reset_index().plot("index", y=label_header_0,kind="scatter", label = 2)
        
        plt.figure()
        plt.scatter(x=sample.index, y=sample[label_header_1],label = label_header_1, marker= "+")
        plt.scatter(x=sample.index, y=sample[label_header_0],label = label_header_0, marker= "+")

        plt.title("Feature: " + feature_header)
        plt.xlabel("index")
        plt.ylabel(feature_header)
        #右上角显示图例
        plt.legend(loc='upper right')
        #在 plt.show() 后实际上已经创建了一个新的空白的图片（坐标轴），
        #这时候你再 plt.savefig() 就会保存这个新生成的空白图片。
        self.__plt_show(plt, feature_header)  
        '''
        current_time = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
        graph_file_path = output_graph_path + feature_header + current_time
        plt.savefig(graph_file_path)
        plt.show()
        '''
    
    def show(self):
        label_header = self.label_header
        for cloumns_header, cloumns_data in self.train_data.iteritems():
            print(cloumns_header)
            #if cloumns_header == "Age":
            #if cloumns_header == "AccountStatus":
            #cloumns_data == pandas.core.series.Series 
            if cloumns_data.dtype == "object":
                None
                self.plt_factor_bar(self.train_data[[label_header, cloumns_header]], label_header, cloumns_header)
            
            elif cloumns_data.dtype == "int64":
                if cloumns_data.value_counts().size < 6:
                    None
                    self.plt_factor_bar(self.train_data[[label_header, cloumns_header]], label_header, cloumns_header)
                else:
                    self.plt_continuous_scatter(self.train_data[[label_header, cloumns_header]], label_header, cloumns_header)
                    self.plt_continuous_kde(self.train_data[[label_header, cloumns_header]], label_header, cloumns_header)
            else:
                print(cloumns_header)
            

    def __plt_show(self, plt, feature_header):
        #在 plt.show() 后实际上已经创建了一个新的空白的图片（坐标轴），
        #这时候你再 plt.savefig() 就会保存这个新生成的空白图片。
        current_time = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
        graph_file_path = self.output_graph_path + feature_header + current_time
        plt.savefig(graph_file_path)
        if self.debug == True:
            plt.show() 
    
#auto_plot = AutoPlot("Target","G:/taodata/Atom/GermanCredit/train.csv", debug = True)
#auto_plot.plt_continuous_kde(auto_plot.train_data[["Age", "Target"]], "Target","Age")

#auto_plot.show()


if 0:
    for cloumns_header, cloumns_data in train_data.iteritems():
        print(cloumns_header)
        #if cloumns_header == "Age":
        #if cloumns_header == "AccountStatus":
        #cloumns_data == pandas.core.series.Series 
        '''
        if cloumns_data.dtype == "object":
            None
            plt_factor_bar(train_data[[label_header, cloumns_header]], label_header, cloumns_header)
        elif cloumns_data.dtype == "int64":
            if cloumns_data.value_counts().size < 6:
                None
                plt_factor_bar(train_data[[label_header, cloumns_header]], label_header, cloumns_header)
            else:
                plt_continuous_scatter(train_data[[label_header, cloumns_header]], label_header, cloumns_header)
                plt_continuous_kde(train_data[[label_header, cloumns_header]], label_header, cloumns_header)
        else:
            print(cloumns_header)
        '''
        if cloumns_header == "PresentResidenceSince":
            plt_factor_bar(train_data[[label_header, cloumns_header]], label_header, cloumns_header)
            plt_continuous_scatter(train_data[[label_header, cloumns_header]], label_header, cloumns_header)
            plt_continuous_kde(train_data[[label_header, cloumns_header]], label_header, cloumns_header)



if 0:
    pdAccountStatus = train_data[["Target", "AccountStatus"]]
    pdAccount_A11 = pdAccountStatus.Target[pdAccountStatus.AccountStatus == "A11"].value_counts()
    pdAccount_A12 = pdAccountStatus.Target[pdAccountStatus.AccountStatus == "A12"].value_counts()
    pdAccount_A13 = pdAccountStatus.Target[pdAccountStatus.AccountStatus == "A13"].value_counts()
    pdAccount_A14 = pdAccountStatus.Target[pdAccountStatus.AccountStatus == "A14"].value_counts()
    psAccount = pd.DataFrame({"A11":pdAccount_A11, "A12":pdAccount_A12, "A13":pdAccount_A13, "A14":pdAccount_A14})
    #Pclass_Survived = pd.DataFrame({u'1等仓':Survived_Pclass_1, u'2等仓':Survived_Pclass_2, u'3等仓':Survived_Pclass_3})
    psAccount.plot(kind = "bar", stacked = True)          #堆积柱状图
    plt.title(u"AccountStatus")
    plt.xlabel(u"target")
    plt.ylabel(u"count")
    plt.show() 
    
    print()

    #from pylab import *
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['axes.unicode_minus'] = False

    pdOffLineTrain = pd.read_csv('ccf_offline_stage1_train.csv', iterator = True)
    #pdOnLineTrain = pd.read_csv('ccf_online_stage1_train.csv')
    '''
    User_id          int64
    Merchant_id      int64
    Coupon_id        object
    Discount_rate    object
    Distance         object
    Date_received    object
    Date             object
    '''

    data_train = pdOffLineTrain.get_chunk(100000)
    #chunk = data.get_chunk(5)
    data_train.info()
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
    '''
    '''
    print('User_id count:', data_train.User_id[data_train.User_id != 0].count())
    print('Merchant_id count:', data_train.Merchant_id[data_train.Merchant_id != 0].count())
    print('Coupon_id count:', data_train.Coupon_id[data_train.Coupon_id != 'null'].count())
    print('Discount_rate count:', data_train.Discount_rate[data_train.Discount_rate != 'null'].count())
    print('Distance count:', data_train.Distance[data_train.Distance != 'null'].count())
    print('Date_received count:', data_train.Date_received[data_train.Date_received != 'null'].count())
    print('Date count:', data_train.Date[data_train.Date != 'null'].count())

    User_id count: 1754884
    Merchant_id count: 1754884
    Coupon_id count: 1053282
    Discount_rate count: 1053282
    Distance count: 1648881
    Date_received count: 1053282
    Date count: 776984
    '''
    feture1 = data_train[['User_id']]
    feture1['User_id_count'] = 1
    feture1 = feture1.groupby('User_id').agg('sum').reset_index()
    print(feture1.describe())
    print(feture1.head(20))

    feture2 = data_train[(data_train.Date !='null')&(data_train.Date_received !='null')][['User_id', 'Coupon_id', 'Date']]
    print(feture2.describe())
    print(feture2.head(20))
    #print(data_train.User_id[data_train.User_id == 165].value_counts())

    #print(data_train.size())
    #会统计出各列的：计数，平均数，方差，最小值，最大值，以及quantile数值
    #print('data_train.describe():\n', data_train.describe())

    #logistic()
    def SibSp():
        fig2 = plt.figure(2)
        fig2.set_figheight(8)
        fig2.set_figwidth(12)

        plt.figure(2)
        #样本中堂兄弟个数分布 柱形图
        plt.subplot2grid((2, 2), (0, 0))
        print(data_train['SibSp'].value_counts())
        #print(data_train.SibSp.value_counts())
        data_train['SibSp'].value_counts().plot(kind = 'bar')
        plt.title(u"SibSp（堂兄弟）分布")
        plt.xlabel(u'堂兄弟个数')
        plt.ylabel(u'样本数')

        #存活的和死亡的样本中堂兄弟分布 柱形图
        plt.subplot2grid((2,2), (0,1))
        plt.title(u'存货SibSp（堂兄弟）分布')
        plt.xlabel(u'堂兄弟个数')
        plt.ylabel(u'样本数')
        #SibSp_Survived = data_train[['Survived', 'SibSp']].groupby('Survived')['SibSp']
        #print(list(SibSp_Survived).value_counts())
        print(data_train[['Survived', 'SibSp']].groupby('Survived')['SibSp'].value_counts())
        #print(data_train[['Survived', 'SibSp']].value_counts())
        #data_train[['Survived', 'SibSp']].groupby('Survived')['SibSp'].value_counts().plot(kind = 'bar')
        data_train.SibSp[data_train.Survived == 1].value_counts().plot(kind = 'bar')

        #存活的和死亡的样本中堂兄弟分布 密度图
        plt.subplot2grid((2, 2), (1, 0))
        plt.title(u'存活的和死亡的样本中堂兄弟分布')
        plt.xlabel(u'堂兄弟个数')
        plt.ylabel(u'样本数')
        #print(data_train.SibSp[data_train.Survived == 1])
        data_train.SibSp[data_train.Survived == 1].plot(kind = 'kde')
        plt.show()

    def bar_stacked():
        fig = plt.figure()
        fig.set(alpha=0.2)  # 设定图表颜色alpha参数
        fig.set_figheight(8)
        fig.set_figwidth(12)

        # 各乘客等级的获救情况 x为乘客等级,通过矩阵转置可以获得
        '''
        Pclass_Survived_0 = data_train.Pclass[data_train.Survived == 0].value_counts()
        Pclass_Survived_1 = data_train.Pclass[data_train.Survived == 1].value_counts()
        Pclass_Survived = pd.DataFrame({u'获救':Pclass_Survived_1, u'未获救':Pclass_Survived_0})
        print('line.',sys._getframe().f_lineno,':Pclass_Survived:\n', Pclass_Survived)
        Pclass_Survived.plot(kind = "bar", stacked = True)          #堆积柱状图
        plt.title(u"各乘客等级的获救情况")
        plt.xlabel(u"乘客等级")
        plt.ylabel(u"人数")
        plt.show()
        '''

        # 各乘客等级的获救情况 x为是否获救
        Survived_Pclass_1 = data_train.Survived[data_train.Pclass == 1].value_counts()
        Survived_Pclass_2 = data_train.Survived[data_train.Pclass == 2].value_counts()
        Survived_Pclass_3 = data_train.Survived[data_train.Pclass == 3].value_counts()

        Pclass_Survived = pd.DataFrame({u'1等仓':Survived_Pclass_1, u'2等仓':Survived_Pclass_2, u'3等仓':Survived_Pclass_3})
        print('line.',sys._getframe().f_lineno,':Pclass_Survived:\n', Pclass_Survived)
        Pclass_Survived.plot(kind = "bar", stacked = True)          #堆积柱状图
        plt.title(u"各乘客等级的获救情况")
        plt.xlabel(u"是否获救")
        plt.ylabel(u"人数")
        plt.show()



        # 不同性别的获救情况 x为是否获救
        Survived_Sex_f = data_train.Survived[data_train.Sex == 'female'].value_counts()
        Survived_Sex_m = data_train.Survived[data_train.Sex == 'male'].value_counts()
        Survived_Sex = pd.DataFrame({u'女性':Survived_Sex_f, u'男性':Survived_Sex_m})
        print('line.',sys._getframe().f_lineno,':Survived_Sex:\n', Survived_Sex)
        Survived_Sex.plot(kind = 'bar', stacked = True)
        plt.title(u'不同性别的获救情况')
        plt.ylabel(u'人数')
        plt.xlabel(u'是否获救')
        plt.show()

        Survived_Embarked_S = data_train.Survived[data_train.Embarked == 'S'].value_counts()
        Survived_Embarked_C = data_train.Survived[data_train.Embarked == 'C'].value_counts()
        Survived_Embarked_Q = data_train.Survived[data_train.Embarked == 'Q'].value_counts()
        Survived_Embarked = pd.DataFrame({'S':Survived_Embarked_S, 'C':Survived_Embarked_C, 'Q':Survived_Embarked_Q})
        print('line.',sys._getframe().f_lineno,':Survived_Sex:\n', Survived_Embarked)
        Survived_Embarked.plot(kind = 'bar', stacked = True)
        plt.title(u'不同登陆口岸的获救情况')
        plt.xlabel(u'是否获救')
        plt.ylabel(u'口岸类型')

        plt.show()

        fig = plt.figure()
        fig.set(alpha=0.2)  # 设定图表颜色alpha参数
        fig.set_figheight(8)
        fig.set_figwidth(12)

        plt.title(u"根据舱等级和性别的获救情况")
        ax1 = fig.add_subplot(141)
        Survived_Sex_F_Pclass_H = data_train.Survived[data_train.Sex == 'female' ][ data_train.Pclass != 3].value_counts()
        print('line.',sys._getframe().f_lineno,':Survived_Sex_F_Pclass_H:\n', Survived_Sex_F_Pclass_H)
        Survived_Sex_F_Pclass_H.plot(kind = 'bar', label = 'female_highclass', color = 'lightpink' )
        ax1.set_xticklabels([u"1", u"0"], rotation=1) #增加x轴说明 x轴的说明顺序需要和原始数据人工对应
        plt.legend([u"女性/高级舱"], loc='best')       #增加柱形说明

        ax2 = fig.add_subplot(142, sharey=ax1)  # sharey=ax1确保y值间隔一致
        Survived_Sex_F_Pclass_L = data_train.Survived[data_train.Sex == 'female' ][ data_train.Pclass == 3].value_counts()
        print('line.',sys._getframe().f_lineno,':Survived_Sex_F_Pclass_L:\n', Survived_Sex_F_Pclass_L)
        Survived_Sex_F_Pclass_L.plot(kind = 'bar', label = 'female_lowclass', color = 'red' )
        ax2.set_xticklabels([u"1", u"0"], rotation=1) #增加x轴说明
        plt.legend([u"女性/低级舱"], loc='best')       #增加柱形说明

        ax3 = fig.add_subplot(143, sharey=ax1)
        Survived_Sex_M_Pclass_H = data_train.Survived[data_train.Sex == 'male' ][ data_train.Pclass != 3].value_counts()
        print('line.',sys._getframe().f_lineno,':Survived_Sex_M_Pclass_H:\n', Survived_Sex_M_Pclass_H)
        Survived_Sex_M_Pclass_H.plot(kind = 'bar', label = 'male_highclass', color = 'lightblue' )
        ax3.set_xticklabels([u"0", u"1"], rotation=1) #增加x轴说明
        plt.legend([u"男性/高级舱"], loc='best')       #增加柱形说明


        ax4 = fig.add_subplot(144, sharey=ax1)
        Survived_Sex_M_Pclass_L = data_train.Survived[data_train.Sex == 'male' ][ data_train.Pclass == 3].value_counts()
        print('line.',sys._getframe().f_lineno,':Survived_Sex_M_Pclass_L:\n', Survived_Sex_M_Pclass_L)
        Survived_Sex_M_Pclass_L.plot(kind = 'bar', label = 'female_lowclass', color = 'steelblue' )
        ax4.set_xticklabels([u"0", u"1"], rotation=1) #增加x轴说明
        plt.legend([u"男性/低级舱"], loc='best')       #增加柱形说明

        plt.show()

        fig = plt.figure()
        fig.set(alpha=0.2)  # 设定图表颜色alpha参数
        data_train.Age[data_train.Survived == 0].plot(kind='kde')
        data_train.Age[data_train.Survived == 1].plot(kind='kde')
        plt.xlabel(u"年龄")# plots an axis lable
        plt.ylabel(u"密度")
        plt.title(u"是否获救的乘客年龄密度分布")
        plt.legend((u'未获救', u'获救'),loc='best') # sets our legend for our graph.
        plt.show()

        fig = plt.figure()
        fig.set(alpha=0.2)  # 设定图表颜色alpha参数
        data_train.Fare[data_train.Survived == 0].plot(kind='kde')
        data_train.Fare[data_train.Survived == 1].plot(kind='kde')
        plt.xlabel(u"票价")# plots an axis lable
        plt.ylabel(u"密度")
        plt.title(u"是否获救的票价密度分布")
        plt.legend((u'未获救', u'获救'),loc='best') # sets our legend for our graph.
        plt.show()


    def base():

        #print(data_train.Survived.value_counts())
        fig = plt.figure(1)                             # 创建图表1
        fig.set(alpha=0.2)                              # 设定图表颜色alpha参数
        fig.set_figheight(8)                            # 设定图表高度为8
        fig.set_figwidth(12)                            # 设定图表宽度为12
        #print(fig.get_figheight(), fig.get_figwidth())# 获取图表的高和宽（默认4*6）
        #plt.figure(1)                                   # 选择图表1
        ax1 = plt.subplot2grid((2,3),(0,0))             # 在一张大图里分列几个小图 2*3的布局，0行1列
        ax2 = plt.subplot2grid((2,3),(0,1))             # 2*3的布局，0行1列
        #data_train.Survived.value_counts().plot(kind='bar', grid='on', ax = ax1)# 柱状图
        plt.sca(ax1)                                    # 选择图表1的子图1
        data_train.Survived.value_counts().plot(kind='bar')# bar:柱状图 line:折线图 barh:横着的柱状图 kde:密度图（概率分布图）
        plt.title(u"获救情况 (1为获救)")                  # 子图标题
        plt.ylabel(u"人数")                              # y轴标题
        plt.xlabel(u"是否获救")

        #plt.subplot2grid((2,3),(0,1))
        plt.sca(ax2)

        data_train.Pclass.value_counts().plot(kind="bar")
        plt.ylabel(u"人数")
        plt.xlabel(u'等级')
        plt.title(u"乘客等级分布")

        plt.subplot2grid((2,3),(0,2))
        plt.scatter(data_train.Survived, data_train.Age) # 散点图函数
        plt.ylabel(u"年龄")                         # 设定纵坐标名称
        plt.grid(b=True, which='major', axis='y')  # 加入网格
        plt.title(u"按年龄看获救分布 (1为获救)")

        plt.subplot2grid((2,3),(1,0), colspan=2)
        data_train.Age[data_train.Pclass == 1].plot(kind='kde')
        data_train.Age[data_train.Pclass == 2].plot(kind='kde')
        data_train.Age[data_train.Pclass == 3].plot(kind='kde')
        plt.xlabel(u"年龄")# plots an axis lable
        plt.ylabel(u"密度")
        plt.title(u"各等级的乘客年龄分布")
        plt.legend((u'头等舱', u'2等舱',u'3等舱'),loc='best') # sets our legend for our graph.


        plt.subplot2grid((2,3),(1,2))
        data_train.Embarked.value_counts().plot(kind='bar')
        plt.title(u"各登船口岸上船人数")
        plt.ylabel(u"人数")

        #plt.show()

        fig = plt.figure(2)                             # 创建图表1
        fig.set(alpha=0.2)                              # 设定图表颜色alpha参数
        fig.set_figheight(8)                            # 设定图表高度为8
        fig.set_figwidth(12)                            # 设定图表宽度为12

        print('line.',sys._getframe().f_lineno, ':Fare 0 describe:' ,train_data.Fare[data_train.Survived ==0].describe())
        print('line.',sys._getframe().f_lineno, ':Fare 1 describe:' ,train_data.Fare[data_train.Survived ==1].describe())
        plt.subplot2grid((2,2), (0,0))
        data_train.Fare[data_train.Survived == 0].plot(kind = 'kde')
        data_train.Fare[data_train.Survived == 1].plot(kind = 'kde')
        plt.title(u'票价密度分布')
        plt.ylabel(u'密度')
        plt.xlabel(u'票价')
        plt.legend((u'未获救0', u'获救1'), loc='best')

        plt.subplot2grid((2,2),(0,1))
        #print('sdf',data_train.Cabin[pd.isnull(data_train.Cabin)])
        print('sdf',data_train.Cabin.count())
        #data_train.Cabin[pd.isnull(data_train.Cabin)].value_counts().plot(kind='bar')
        Survived_Cabin_notnull = data_train.Survived[pd.notnull(data_train.Cabin)].value_counts()
        Survived_Cabin_null = data_train.Survived[pd.isnull(data_train.Cabin)].value_counts()
        Survived_Cabin = pd.DataFrame({u'Cabin':Survived_Cabin_notnull, u'NoCabin':Survived_Cabin_null}) #这里有个transpose()非常关键
        #矩阵形成字典数据时，Cabin 和NoCabin作为dataframe的列标题 是否获救（0,1）作为样本行标题
        #柱状图用矩阵的行标题作为X轴，列标题作为标记，样本数量作为y轴，transpose为转置
        print('line.',sys._getframe().f_lineno, ':Survived_Cabin:\n', Survived_Cabin)
        print('line.',sys._getframe().f_lineno, ':Survived_Cabin.transpose:\n', Survived_Cabin.transpose())
        Survived_Cabin.transpose().plot(kind = 'bar', stacked = True)

        plt.title(u'船舱位置统计')
        plt.ylabel(u'个数')
        plt.xlabel(u'船舱')
        plt.show()

    def set_Cabin_type(df):
        #一、选取标签为A和C的列，并且选完类型还是dataframe
        #df = df.loc[:, ['A', 'C']]
        df.loc[ (df.Cabin.notnull()), 'Cabin' ] = "Yes"
        df.loc[ (df.Cabin.isnull()), 'Cabin' ] = "No"
        return df

    def data_deal(data):
        data_train_tmp = data
        #data_feature = data
        data_train_tmp = set_Cabin_type(data_train_tmp)
        dummies_Cabin = pd.get_dummies(data_train_tmp['Cabin'], prefix= 'Cabin')
        #print(dummies_Cabin)
        dummies_Sex = pd.get_dummies(data_train_tmp['Sex'], prefix = 'Sex')
        dummies_Emberked = pd.get_dummies(data_train_tmp['Embarked'], prefix = 'Embarked')
        dummies_Pclass = pd.get_dummies(data_train_tmp['Pclass'], prefix = 'Pclass')
        #dummies_Emberked = pd.get_dummies(data_train_tmp['Embarked'], prefix = 'Embarked')
        #指定轴方向，默认纵向合并（合并样本），axis=1时是横向合并（合并特征向量）
        data_feature = pd.concat([dummies_Sex, dummies_Pclass, dummies_Emberked, dummies_Cabin], axis=1)
        data_feature['Fare'] = data.Fare
        print(data_feature.info())


        return data_feature

    def logistic():
        train_feature = data_deal(data_train)

        #test_np = data_deal(data_test)

        #print(dummies_Sex)
        #scaler = preprocessing.StandardScaler()
        scaler = preprocessing.StandardScaler()
        #print(data_train_tmp.Fare)
        #age_scale_param = scaler.fit(data_train['Age'])
        #df['Age_scaled'] =scaler.fit scaler.fit_transform(df['Age'], age_scale_param)
        fare_scale_param = scaler.fit(train_feature.Fare.values.reshape(-1, 1))
        #print(fare_scale_param)
        train_feature['Fare_scaled'] = scaler.fit_transform(train_feature['Fare'].values.reshape(-1, 1), fare_scale_param)
        train_feature['Survived'] = data_train.Survived
        #print(data_feature.describe())
        # 用正则取出我们要的属性值 把需要的feature字段取出来，转成numpy格式
        train_df = train_feature.filter(regex='Survived|Fare_.*|Cabin_.*|Embarked_.*|Sex_.*|Pclass_.*')

        test_feature = data_deal(data_test)
        test_feature.loc[(test_feature.Fare.isnull()), 'Fare'] = 0
        test_feature['Fare_scaled'] = scaler.fit_transform(test_feature['Fare'].values.reshape(-1, 1), fare_scale_param)
        test_df = test_feature.filter(regex='Fare_.*|Cabin_.*|Embarked_.*|Sex_.*|Pclass_.*')
        print('test_df\n:',test_df.head())
        print('train_df\n:',train_df.head())
        #as.matrix一般是将整个数据框(各列数据类型必须相同，否则进行强制转换)转换为维数相同的矩阵，目的是为了利用那些矩阵运算的函数
        train_np = train_df.as_matrix()
        x_feature = train_np[:, :11]
        y_feature = train_np[:, 11]#不能直接使用train_np[11]，结果是样本
        # fit到RandomForestRegressor之中
        clf = linear_model.LogisticRegression(C=1.0, penalty='l1', tol=1e-6)
        clf.fit(x_feature, y_feature)
        print(clf)

        test_np = test_df.as_matrix()

        predictions = clf.predict(test_np)
        data_grade = pd.DataFrame({"columns":list(train_df.columns)[0:11], "coef":list(clf.coef_.T)})
        print(data_grade)
        #result = pd.DataFrame({'PassengerId':data_test['PassengerId'].as_matrix(), 'Survived':predictions.astype(np.int32)})
        #result.to_csv("logistic_regression_predictions.csv", index=False)

