# coding: utf-8
import numpy as np #为了方便使用numpy 采用np简写

def create_array():
    array = np.array([[1,2,3],[2,3,4]])  #列表转化为矩阵
    print(array)
    """
    array([[1, 2, 3],
           [2, 3, 4]])
    """
    print('number of dim:',array.ndim)  # 维度

    a = np.array([2,23,4],dtype=np.float)
    print(a.dtype)
    b = np.zeros((3,4))
    c = np.ones((3,4),dtype = np.int)   # 数据为1，3行4列
    d = np.empty((3,4)) # 数据为empty，3行4列
    e = np.arange(10,20,2) # 10-19 的数据，2步长
    f = np.arange(12).reshape((3,4))    # 3行4列，0到11
    g = np.linspace(1,10,20)    # 开始端1，结束端10，且分割成20个数据，生成线段
    h = np.linspace(1,10,20).reshape((5,4)) # 更改shape


a=np.array([10,20,30,40])   # array([10, 20, 30, 40])
b=np.arange(4)              # array([0, 1, 2, 3])
c=a-b  # array([10, 19, 28, 37])
c=a-b  # array([10, 19, 28, 37])
c=a*b   # array([  0,  20,  60, 120])
c=b**2  # array([0, 1, 4, 9])
c=10*np.sin(a)
print(b<3)

a=np.array([[1,1],[0,1]])
b=np.arange(4).reshape((2,2))
c_dot = np.dot(a,b)
c_dot_2 = a.dot(b)

a=np.random.random((2,4))
print(a)
np.sum(a)
np.min(a)
np.max(a)
print("a =",a)
print("sum =",np.sum(a,axis=1))
print("min =",np.min(a,axis=0))
print("max =",np.max(a,axis=1))

A = np.arange(2, 14).reshape((3, 4))

# array([[ 2, 3, 4, 5]
#        [ 6, 7, 8, 9]
#        [10,11,12,13]])

print(np.argmin(A))  # 0
print(np.argmax(A))  # 11
print(np.mean(A))        # 7.5
print(np.average(A))     # 7.5
print(A.mean())          # 7.5
print(A.median())       # 7.5
print(np.cumsum(A)) # [2 5 9 14 20 27 35 44 54 65 77 90]
print(np.diff(A))
# [[1 1 1]
#  [1 1 1]
#  [1 1 1]]
A = np.arange(14,2, -1).reshape((3,4))
# array([[14, 13, 12, 11],
#       [10,  9,  8,  7],
#       [ 6,  5,  4,  3]])

print(np.sort(A))

# array([[11,12,13,14]
#        [ 7, 8, 9,10]
#        [ 3, 4, 5, 6]])
print(np.transpose(A))
print(A.T)

# array([[14,10, 6]
#        [13, 9, 5]
#        [12, 8, 4]
#        [11, 7, 3]])
# array([[14,10, 6]
#        [13, 9, 5]
#        [12, 8, 4]
#        [11, 7, 3]])

A.flatten()


