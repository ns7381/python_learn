# coding: utf-8
import argparse
import os
import subprocess
import sys
import threading
import time
from datetime import date, timedelta

from pyspark.sql import SparkSession

# 设置utf编码
reload(sys)
sys.setdefaultencoding('utf-8')
current_dir = os.getcwd()

# 参数解析
parser = argparse.ArgumentParser()
parser.add_argument("--cluster", default='test', help="execute sql in this cluster")
parser.add_argument("--queue", default='test', help="execute sql in this queue")
parser.add_argument("--dt", default=str(date.today() - timedelta(days=1)), help="execute sql on this day")
parser.add_argument("--max_threads", default=32, help="execute max thread number")
args = parser.parse_args()

# 提取hive环境变量
my_env = os.environ.copy()
HIVE_ENV = '''
export HIVE_CONF_DIR={HIVE_CONF_DIR};
export SPARK_CONF_DIR={SPARK_CONF_DIR};
export HIVE_HOME={HIVE_HOME};
'''.format(HIVE_CONF_DIR=my_env['HIVE_CONF_DIR'],
           SPARK_CONF_DIR=my_env['SPARK_CONF_DIR'],
           HIVE_HOME=my_env['HIVE_HOME'])

# 初始化
success_count = 0
fail_count = 0
max_threads = args.max_threads
threads = list()
lock = threading.Lock()
split_line = "#" * 60 + " 第%d条sql " + "#" * 60 + "\n"
start = time.time()

# 开启sparkSession, 根据参数获取线上sql记录
spark = SparkSession \
    .builder \
    .appName("user-sql-explain") \
    .enableHiveSupport() \
    .getOrCreate()
sqlDF = spark.sql('''
select 
    current_db, env, sql 
from fdm.fdm_m99_aiops_job_conf_di 
where 
    sql is not null and 
    sql != '' and
    env not rlike '.*add jar file:(?!/software/udf|/home/udf).*' and
    cluster = '{cluster}' and 
    queue = '{queue}' and
    dt = '{dt}'
'''.format(cluster=args.cluster, queue=args.queue, dt=args.dt))

sqlDs = sqlDF.rdd \
    .filter(lambda row: ";" not in row.sql) \
    .map(lambda row: "%s use %s;explain %s;" % (row.env, row.current_db or "default", row.sql))

# 记录hive执行语句输出结果
success_log = open(current_dir + "/success.log", "w")
fail_log = open(current_dir + "/fail.log", "w")


# 按照线程个数均分要执行的sql记录
def chunk(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


# 执行hive-sql语句
def run_hive_cmd(sql_list):
    global success_count, fail_count
    for sql in sql_list:
        sql = sql.replace('\\', '\\\\\\\\\\').replace('"', "\\\\\\\"").replace("`", "\\`")
        cmd = HIVE_ENV + 'hive -e "%s"' % sql
        pipe = subprocess.Popen(cmd, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res = pipe.communicate()
        lock.acquire()
        try:
            if pipe.returncode == 0:
                success_count += 1
                success_log.write(split_line % success_count)
                success_log.write("执行成功: " + cmd + "\n")
                success_log.write(res[0].decode(encoding='utf-8'))
                print('线程[%s]成功执行第%d条sql' % (threading.current_thread().name, success_count))
            else:
                fail_count += 1
                fail_log.write(split_line % fail_count)
                fail_log.write("执行失败: " + cmd)
                fail_log.write(res[1].decode(encoding='utf-8'))
                print('线程[%s]失败执行第%d条sql' % (threading.current_thread().name, fail_count))
        finally:
            lock.release()


# 开启线程并发执行
for records in chunk(sqlDs.collect(), max_threads):
    thread = threading.Thread(target=run_hive_cmd, args=(records,))
    threads.append(thread)
    thread.start()

print("启动线程个数: %d" % len(threads))
for t in threads:
    t.join()

# 输出执行结果
end = time.time()
hours, rem = divmod(end - start, 3600)
minutes, seconds = divmod(rem, 60)
print("耗时 {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
print("执行成功：{success}, 执行失败 {fail}".format(success=success_count, fail=fail_count))
success_log.close()
fail_log.close()
