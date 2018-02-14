import pandas as pd
import numpy as np
def test1():
    '''
    pandas入门
    '''
    pdSeries = pd.Series([1,2,3,4,5,6])
    print(pdSeries)
    dates = pd.date_range('2016.8.1', periods = 6)
    print('datas:\n', dates)

    pdIndex = [1,2,3,4,5]
    pdColumns = list('ABCD')
    npJuzheng = np.random.randn(len(pdIndex), len(pdColumns))
    print('npJuzheng', npJuzheng)
    df = pd.DataFrame(npJuzheng, index = pdIndex, columns = pdColumns)
    print('df:\n', df)

def test2():
    '''
    #通过一维数组创建Series序列
    '''
    print('通过一维数组创建Series序列')
    print('np.arange(10):\n', np.arange(10))
    print('pd.Series(np.arange(10)):\n', pd.Series(np.arange(10)))
    #通过字典创建Series序列
    print('通过{字典}创建Series序列')
    dicArray = {'A':11, 'B':22, 'C':33, 'D':44}
    print('dicArray{}:\n', dicArray)
    print('pd.Series(dicArray)):\n', pd.Series(dicArray))
def test3():
    '''
    #通过二维数组创建DateFrame序列
    '''
    print('通过二维数组创建DateFrame序列')
    print('np.array(np.arange(15)).reshape(5, 3):\n', np.array(np.arange(15)).reshape(5, 3))
    print('pd.DataFrame(np.array(np.arange(15)).reshape(5, 3)):\n', pd.DataFrame(np.array(np.arange(15)).reshape(5, 3)))
    #通过数组字典创建DateFrame序列
    print('通过数组字典创建DateFrame序列')
    dicValue = np.arange(12).reshape(3, 4)
    dicArray = {'A':dicValue[0], 'B':dicValue[1], 'C':dicValue[2]}
    print('dicArray{}:',dicArray)
    print('pd.DataFrame(dicArray)：\n', pd.DataFrame(dicArray))
def test4():
    '''
    #普通索引：通过索引值或索引标签获取数据
    print('普通索引：通过索引值或索引标签获取数据')
    '''
    pdSeries = pd.Series(np.arange(5))
    print('pd.Series(np.arange(5)):\n', pdSeries)
    print('！！如果通过索引标签获取数据的话，末端标签所对应的值可以返回')
    print('pdSeries[4]:', pdSeries[4])
    pdSeries.index = list('abcde')
    print('pdSeries.index = list(\'abcde\'):\n', pdSeries)
    print('pdSeries[\'e\']:', pdSeries['e'])
    for i in pdSeries['c':]:
        print('pdSeries[\'c\':]:', i)
def test5():
    '''
    #自动化对齐
    '''
    pdSeries1 = pd.Series(np.arange(5))
    #值拷贝 
    pdSeries2 = pdSeries1.copy()
    pdSeries1.index = list('abcde')
    pdSeries2.index = list('edcba')
    print('pdSeries1:\n', pdSeries1)
    print('pdSeries2:\n', pdSeries2)
    print('pdSeries1 + pdSeries2:', pdSeries1 + pdSeries2)

def test6():
    '''
    #frame数据查询
    '''
    pdFrame = pd.DataFrame(np.arange(30).reshape(5, 6))
    pdFrame.columns = ('a', 'b', 'c', 'd', 'e', 'f')
    print('pdFrame:\n', pdFrame)
    print('pdFrame[[\'d\', \'e\']]:查看指定列数据\n', pdFrame[['d', 'e']])
    print('pdFrame.ix[0:2]:查看指定行数据\n', pdFrame.ix[0:2])
    print('pdFrame.ix[0:2, [\'d\',\'e\']]查看指定行列数据\n', pdFrame.ix[0:2, ['d','e']])
def test7():
    '''
    test7:frame数据增删改
    '''
    pdFrame1 = pd.DataFrame(np.arange(10, 22, 1).reshape(3,4))
    pdFrame1.columns = list('abcd')
    #list('abc')
    pdFrame2 = pd.DataFrame(np.arange(20, 32, 1).reshape(3,4))
    pdFrame2.columns = list('abce')
    print('pdFram1:\n', pdFrame1)
    print('pdFram2:\n', pdFrame2)
    print('pd.concat([pdFrame1, pdFrame2]).reset_index():新增数据行(.reset_index()必须重置索引)\n', pd.concat([pdFrame1, pdFrame2]).reset_index())
    print('pdFrame1.drop(1):删除指定行\n',pdFrame1.drop(1))
    pdFrame1['NewColums'] = 9
    print('pdFrame1[\'NewColums\'] = 9:新增一列数据\n',pdFrame1)
    print('pdFrame1.drop([\'a\'], axis = 1):删除指定列\n', pdFrame1.drop(['a'], axis = 1))
    pdFrame1.ix[2, 'c'] = 100
    pdFrame1.ix[pdFrame1['b'] == 15, 'c'] = 200
    print('修改指定行列数据')
    print('pdFrame1.ix[2, \'c\'] = 100&&\n pdFrame1.ix[pdFrame1[\'b\'] == 15, \'c\'] = 200:\n', pdFrame1)
def test8():
    '''
    test8:groupby()函数实现数据的聚合操作
    '''
    pdFrame = pd.DataFrame(columns = ['name', 'age', 'sex', 'height', 'weight', 'score'])
    pdFrame['name'] = ['fang', 'sheng', 'jie', 'jiang', 'chun']
    pdFrame['age'] = range(17, 22)
    pdFrame['sex'] = ['M', 'M', 'M','F', 'F']
    pdFrame['height'] = np.random.randint(160, 180, 5)
    pdFrame['weight'] = np.random.randint(90, 120, 5)
    pdFrame['score'] = np.random.randint(90, 100, 5)
    pdFrame.to_csv('pdFrame.csv', index = None)
    print('pdFrame:\n', pdFrame)
    #如果不对原始数据作限制的话，聚合函数会自动选择数值型数据进行聚合计算。如果不想对年龄计算平均值的话，就需要剔除改变量
    print('pdFrame.drop([\'age\', \'height\'], axis = 1).groupby(\'sex\').mean():\n', pdFrame.drop(['age', 'height'], axis = 1).groupby('sex').mean())
    print('pdFrame.sort_values(by = [\'sex\', \'age\']):\n', pdFrame.sort_values(by = ['sex', 'age']))
    print('list(pdFrame.groupby(\'sex\')[\'age\']):\n', list(pdFrame.groupby('sex')['age']))
    print('pdFrame.groupby(\'sex\')[\'age\'].agg(lambda x: max(x) - min(x)):\n', pdFrame.groupby('sex')['age'].agg(lambda x: max(x) - min(x)))

def test9():
    '''
    map && join && lambda
    '''
    list1 = [1,2,3,4,5]
    print('由于结果r是一个Iterator，Iterator是惰性序列，因此通过list()函数让它把整个序列都计算出来并返回一个list。')
    print('list(map(lambda x:x*x, list1)):\n', list(map(lambda x:x*x, list1)))
    print('\'+\'.join(map(str, list1)):列表转成字符后使用join拼接\n','+'.join(map(str, list1)))
    tuple1 = ((3, 5), (4, 6), (5, 8))
    print('tuple:\n', tuple1, ' to 35+46+58 ')
    print()
    print('\'+\'.join(map(lambda x:\'\'.join(map(str, x)),tuple1))', '+'.join(map(lambda x:''.join(map(str, x)), tuple1)))
def MyTest():
    listFun = [test1, test2, test3, test4, test5, test6, test7]
    for func in listFun:
        print('*****************test', listFun.index(func)+1, '******************')
        print(func.__doc__)
        func()

print(test3.__doc__) 
test3()

#MyTest()
    
print('ok')
input('stop')
