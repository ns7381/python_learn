try:
    f = open('/Users/nathan/project/python/learn/python_learn/unit_test/mydict.py', 'r')
    print(f.read())
finally:
    if f:
        f.close()



with open('/Users/nathan/project/python/learn/python_learn/unit_test/mydict.py', 'r') as f:
    for line in f.readlines():
        print(line.strip())


with open('test.txt', 'w') as f:
    f.write('Hello, world!')


