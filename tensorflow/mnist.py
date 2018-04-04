#识别数字
import tensorflow as tf
import tensorboard as tb
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.examples.tutorials.mnist import input_data
#下载失败，手动下载后读取本地文件
#mnist = input_data.read_data_sets("MNIST_data", one_hot = True)
#import input_data
#onehot之后，label从7变成[000000100]
mnist = input_data.read_data_sets('./', one_hot=True)
#计算精度
def compute_accuracy(x_test, y_test):
    global y_prediction
    placeholder_param = {x_dataset:x_test, y_dataset:y_test}
    y_pre = sess.run(y_prediction, feed_dict={x_dataset:x_test})
#     print(y_pre[0:5])
#     print(np.argmax(y_pre[0:5], axis=1))
#     print(y_test[0:5])
#     print(np.argmax(y_test[0:5], axis=1))
    #tf.argmax 获取矩阵中最大值所在的index或者column index 取决于axis
    #比较两者是否相等，相等则为true，不相等为false
    correct_prediction = tf.equal(tf.argmax(y_pre,axis=1), tf.argmax(y_test, axis=1))
    print("correct_prediction:", sess.run(correct_prediction , feed_dict=placeholder_param))
    #bool型转成float32
    correct_pred_float32 = tf.cast(correct_prediction, tf.float32)
    print("correct_pred_float32:" , sess.run(correct_pred_float32, feed_dict=placeholder_param))
    #计算矩阵总平均值，准确率越高，1越多，则平均值越接近1
    accuracy = tf.reduce_mean(correct_pred_float32)
    print("accuracy:", sess.run(accuracy, feed_dict=placeholder_param))
    result = sess.run(accuracy, feed_dict={x_dataset:x_test, y_dataset:y_test})
    return result

#添加隐藏层
#inputs:输入的x矩阵(55000, 784)
#in_size:输入的x的列个数,即特征个数784
#out_size:输出的x的列个数，即降维后的特征个数10
#n_layer:神经网络的层序列
#activation_fun:激励函数
def add_layer(inputs, in_size, out_size, n_layer, activation_fun = None):
    layer_name = "layer%s"%n_layer
    with tf.name_scope("layer"):
        with tf.name_scope("weights"):
            #weights用正态分布赋初值
            #weights用于降维，
            #data_x *weights: (55000, 784)*(784, 10) = (55000, 10)
            #样本的个数不变，特征个数从784降到了10个
            #在这里，10直接用于数据的label
            #label中每个值都是
            #label第一行第一列 = x的第一行 * w的第一列
            #label[0][0] = x00*w00 + x01*w10 + x01*w20…… x(0,783)*w(783,0) 
            #label第一行第二列 = x的第一行 * w的第二列
            #label[0][1] = x00*w01 + x01*w11 + x01*x21…… x(0,783)*w(783,1) 
            #
            weights = tf.Variable(tf.random_normal([in_size, out_size]), name="W")
            tf.summary.histogram(layer_name + "_weights", weights)
        with tf.name_scope("biases"):
            #b初值不能为0，给一个0.1
            biases = tf.Variable(tf.zeros([1, out_size]) + 0.1, name="b")
            tf.summary.histogram(layer_name + "_biases", biases)
        with tf.name_scope("wx_plus_b"):
            wx_plus_b = tf.matmul(inputs, weights) + biases
        if activation_fun == None:
            outputs = wx_plus_b
        else:
            outputs = activation_fun(wx_plus_b)
        tf.summary.histogram(layer_name + "_outputs", outputs)   
        return outputs
#怎么看分辨率是28*28=784?
#minist.train.images.shape
#(55000, 784)
#mnist库中的mnist.train.images即为55000行784的一个矩阵
x_variable_num  = len(mnist.train.images[0])
#x_dataset变量定义为行数不限，指定列数为784的矩阵：train样本数不限，特征个数为784个，特征值为像素值 范围(0,1)
#y_dataset变量定义为行数不限，指定列数为10的矩阵：test样本数不限，label类为10个。
x_dataset = tf.placeholder(tf.float32, [None, x_variable_num])
y_dataset = tf.placeholder(tf.float32, [None, 10])
#softmax回归，是逻辑回归在多分类问题上的推广
y_prediction = add_layer(x_dataset, x_variable_num, 10, n_layer = 1, activation_fun = tf.nn.softmax)
#两次reduce降维，把交叉熵错误率的损失函数从二维矩阵降到0维标量
loss = tf.reduce_mean(-tf.reduce_sum(y_dataset * tf.log(y_prediction), reduction_indices=[1]))
#梯度下降法优化损失值为最低
train_step = tf.train.GradientDescentOptimizer(0.5).minimize(loss)
with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())
    #训练次数越多，准确度越高
    for i in range(1000):
        batch_x,batch_y = mnist.train.next_batch(100)
        placeholder_pram = {x_dataset:batch_x, y_dataset:batch_y}
        sess.run(train_step, feed_dict = placeholder_pram)
        if i % 50 ==0:
            print(sess.run(loss, feed_dict = placeholder_pram), compute_accuracy(mnist.test.images, mnist.test.labels))