# coding: utf-8
import subprocess

arr = []
str = 'alter table ms_perf_test add partition(dt="{}");'
sql = 'use spark_test;'
for i in range(0, 1000):
    sql += str.format("2020-01-" + str(i))


cmd = """
hive -e "{sql}"
""".format(sql)
p = subprocess.call(sql, shell=True)

print('返回值是:' + str(p))

if p == 0:
    print('任务运行成功')
else:
    raise Exception('error')
