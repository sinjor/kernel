#识别数字
import tensorflow as tf
import tensorboard as tb
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.examples.tutorials.mnist import input_data
#下载失败，手动下载后读取本地文件
#minist = input_data.read_data_sets("MNIST_data", one_hot = True)
#import input_data
#onehot之后，label从7变成[000000100]
minist = input_data.read_data_sets('./', one_hot=True)

def compute_accuracy(x_test, y_test):
    global y_prediction
    placeholder_param = {x_dataset:x_test, y_dataset:y_test}
    y_pre = sess.run(y_prediction, feed_dict={x_dataset:x_test})
#     print(y_pre[0:5])
#     print(np.argmax(y_pre[0:5], axis=1))
#     print(y_test[0:5])
#     print(np.argmax(y_test[0:5], axis=1))
    #argmax 获取矩阵中最大值所在的index或者column index 取决于axis
    #比较两者是否相等，相等则为true，不相等为false
    correct_prediction = tf.equal(tf.argmax(y_pre,axis=1), tf.argmax(y_test, axis=1))
    print("correct_prediction:", sess.run(correct_prediction , feed_dict=placeholder_param))
    #bool型转成float32
    correct_pred_float32 = tf.cast(correct_prediction, tf.float32)
    print("correct_pred_float32:" , sess.run(correct_pred_float32, feed_dict=placeholder_param))
    #计算矩阵平均值
    accuracy = tf.reduce_mean(correct_pred_float32)
    print("accuracy:", sess.run(accuracy, feed_dict=placeholder_param))
    result = sess.run(accuracy, feed_dict={x_dataset:x_test, y_dataset:y_test})
    return result

def add_layer(inputs, in_size, out_size, n_layer, activation_fun = None):
    layer_name = "layer%s"%n_layer
    with tf.name_scope("layer"):
        with tf.name_scope("weights"):
            weights = tf.Variable(tf.random_normal([in_size, out_size]), name="W")
            tf.summary.histogram(layer_name + "_weights", weights)
        with tf.name_scope("biases"):
            biases = tf.Variable(tf.zeros([1, out_size]) + 0.1, name="b")
            tf.summary.histogram(layer_name + "_biases", biases)
        with tf.name_scope("wx_plus_b"):
            wx_plus_b = tf.matmul(inputs,weights) + biases
        if activation_fun == None:
            outputs = wx_plus_b
        else:
            outputs = activation_fun(wx_plus_b)
        tf.summary.histogram(layer_name + "_outputs", outputs)   
        return outputs
#怎么看分辨率是28*28=784？
x_variable_num  = len(minist.train.images[0])
x_dataset = tf.placeholder(tf.float32, [None, x_variable_num])
y_dataset = tf.placeholder(tf.float32, [None, 10])
y_prediction = add_layer(x_dataset, x_variable_num, 10, n_layer = 1, activation_fun = tf.nn.softmax)
loss = tf.reduce_mean(-tf.reduce_sum(y_dataset * tf.log(y_prediction), reduction_indices=[1]))
train_step = tf.train.GradientDescentOptimizer(0.5).minimize(loss)
with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())
    #训练次数越多，准确度越高
    for i in range(1000):
        batch_x,batch_y = minist.train.next_batch(100)
        placeholder_pram = {x_dataset:batch_x, y_dataset:batch_y}
        sess.run(train_step, feed_dict = placeholder_pram)
        if i % 50 ==0:
            print(sess.run(loss, feed_dict = placeholder_pram), compute_accuracy(minist.test.images, minist.test.labels))