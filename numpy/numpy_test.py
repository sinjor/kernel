import numpy as np
#显示矩阵的全部内容
np.set_printoptions(threshold=np.inf)
def mean_test():
    test_data = np.random.randint(1,6,6).reshape(2, 3)
    print(test_data)
    test_column_mean = test_data.mean(axis=1)
    test_row_mean = test_data.mean(axis=0)
    test_mean = test_data.mean()
    test_column_sum = test_data.sum(axis=1)
    test_row_sum = test_data.sum(axis=0)
    test_sum = test_data.sum()
    #获取矩阵的行列值
    index, column = test_data.shape
    #手工计算平均数，说明矩阵的平均数分为行平均数，列平均数，和总平均数，有axis控制
    test_column_mean_calcu = test_column_sum/column
    test_row_mean_calcu = test_row_sum/index
    test_mean_calcu = test_sum/(column*index)
    print("test_column_mean:", test_column_mean)
    print("test_column_mean_calcu", test_column_mean_calcu)
    print("test_row_mean:", test_row_mean)
    print("test_row_mean_calcu", test_row_mean_calcu)
    print("test_mean:", test_mean)
    print("test_mean_calcu", test_mean_calcu)

#matrix multiplication
def matmul_test():
    #(2, 3)*(3, 2)或者(3, 2)*(2, 3)
    test_data_a = np.random.randint(1, 3, 6).reshape(2, 3)
    test_data_b = np.random.randint(1, 3, 6).reshape(3, 2)
    matmul_ab = np.matmul(test_data_a, test_data_b)
    matmul_ba = np.matmul(test_data_b, test_data_a)
    print("test_data_a:\n", test_data_a)
    print("test_data_b:\n", test_data_b)
    print("matmul_ab:\n", matmul_ab)
    print("matmul_ba:\n", matmul_ba)