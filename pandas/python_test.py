print("apple\n")
listApple = [66.52, 45, 90.23, 1234.9, 0]

print("listApple.count(90.23):", listApple.count(90.23), listApple[0])

if listApple[0] < 30:
    print('listApple < 30', listApple[0])
elif listApple[0]>30 :
    print('listApple >30', listApple[0])

#test2:循环内修改正在迭代的序列   
# 需要加上：如果要在循环内修改正在迭代的序列（例如，复制所选的项目）
#，建议首先创建原序列的拷贝。对序列进行迭代不会隐式地进行复制。
#切片符号使这特别方便
print('\ntest2:循环内修改正在迭代的序列')
listWords = ['fang', 'sheng', 'jie', 'jiang', 'chun', 'fang']
for strWord in listWords[:] :
    print('strWord:', strWord, 'len:', len(strWord))
    if strWord == 'jie':
        listWords.insert(0, strWord)
print('listWords:', listWords)

#test 3遍历一个数字序列
print('\ntest3:遍历一个数字序列')
for i in range(10):
    print('i:', i)
#遍历一个数字序列 从15至25，step = 2
for i in range(15, 25, 2):
    print('i:', i)
else:
    pass#什么都不处理相当于;
    print('for end i:', i)

#test 4创建一个函数
print('#test 4创建一个函数')
def test4(n, m, x):
    for i in n:
        print('i:', i)

    for j in m[:]:
        print('j:', j)
        m.insert(0, 'test4')
    else:
        print('end for j:', j)

    x = 9
    print('n', n, 'm', m, 'x', x)
#listn = range(1, 10, 2)
listn = [1,2,3,4,5,6]
listm = ['fang', 'sheng', 'jie']
x = 15
test4(listn, listm, x)
print('listn:', listn, 'listm:', listm, 'x:', x)

'''
Number（数字）
String（字符串）
Python 没有单独的字符类型，一个字符就是长度为1的字符串
与 C 字符串不同的是，Python 字符串不能被改变。向一个索引位置赋值，比如word[0] = 'm'会导致错误

List（列表）
列表可以完成大多数集合类的数据结构实现。
列表中元素的类型可以不相同，它支持数字，字符串甚至可以包含列表（所谓嵌套）。可以被改变

Tuple（元组）
可以把字符串看作一种特殊的元组。
虽然tuple的元素不可改变，但它可以包含可变的对象，比如list列表。

Sets（集合）
集合（set）是一个无序不重复元素的序列。
基本功能是进行成员关系测试和删除重复元素。

Dictionary（字典）
列表是有序的对象结合，字典是无序的对象集合。两者之间的区别在于：字典当中的元素是通过键来存取的，而不是通过偏移存取。
字典是一种映射类型，字典用"{ }"标识，它是一个无序的键(key) : 值(value)对集合。
键(key)必须使用不可变类型。
在同一个字典中，键(key)必须是唯一的
'''
