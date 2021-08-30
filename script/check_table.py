# coding: utf-8
import argparse
import os
import subprocess
import sys
import threading
import time
from datetime import date, timedelta


# 设置utf编码
reload(sys)
sys.setdefaultencoding('utf-8')
current_dir = os.getcwd()


# 初始化
success_count = 0
fail_count = 0
max_threads = 20
threads = list()
lock = threading.Lock()
split_line = "#" * 60 + " 第%d条sql " + "#" * 60 + "\n"
start = time.time()

# 记录hive执行语句输出结果
success_log = open(current_dir + "/success.log", "w")
fail_log = open(current_dir + "/fail.log", "w")
fail_record = open(current_dir + "/record.txt", "w")
fail_sql = open(current_dir + "/fail_sql.txt", "w")



# 参数解析
parser = argparse.ArgumentParser()
parser.add_argument("--cluster", default='test1', help="execute sql in this cluster")
parser.add_argument("--queue", default='test1', help="execute sql in this queue")
parser.add_argument("--user", default='test1', help="execute user in this queue")
parser.add_argument("--team_user", default='', help="execute user in this queue")
parser.add_argument("--dt", default=str(date.today() - timedelta(days=1)), help="execute sql on this day")
args = parser.parse_args()

# 提取hive环境变量
hive_exec_env = os.environ.copy()
hive_exec_env['USER'] = args.team_user

HIVE_ENV = "export HIVE_HOME=%s;export HIVE_CONF_DIR=%s;" % (hive_exec_env['HIVE_HOME'], hive_exec_env['HIVE_CONF_DIR'])



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
def run_hive_cmd(tbl_list):
    global success_count, fail_count
    for tbl in tbl_list:
        tbl_name = tbl['db'] + "." + tbl['tbl']
        part_name = tbl['part']
        part_names = part_name.split("/")

        sql = ("select * FROM %s") % (tbl_name)
        filt = ' where 1 = 1 '
        for part in part_names:
            filt += ' and %s = "%s" ' % (part.split("=")[0], part.split("=")[1])
        sql += filt + " limit 1;"


        cmd = HIVE_ENV + "hive --hiveconf hive.metastore.uris=thrift://localhost:8080 -e '%s'" % sql
        print(sql)
        pipe = subprocess.Popen(cmd, env=hive_exec_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res = pipe.communicate()
        if pipe.returncode == 0:
            success_count += 1
            success_log.write(split_line % success_count)
            success_log.write("执行成功: " + cmd + "\n")
            success_log.write(res[0].decode(encoding='utf-8'))
            print('线程[%s]成功执行第%d条sql' % (threading.current_thread().name, success_count))
        else:
            fail_count += 1
            fail_log.write(split_line % fail_count)
            fail_log.write("执行失败: " + cmd + "\n")
            fail_log.write(res[1].decode(encoding='utf-8'))
            fail_record.write(tbl['tbl'] + "\n")
            fail_sql.write(cmd + "\n")
            print('线程[%s]失败执行第%d条sql' % (threading.current_thread().name, fail_count))

# 解析文件获得表和分区   | app_ea_del_waybill_d_reverse_rn | dt=2021-03-29/dp=1 |
tbls = list()
with open('/tmp/ns/checktbl/test1.tbls', 'r') as f:
    for line in f.readlines():
        strs = line.strip().split("\t")
        tbls.append({"db": strs[0].strip(), "tbl": strs[1].strip(), "part": strs[2].strip()})

tbls = tbls[2000:]
print("总共执行" + str(len(tbls)))

# 单线程跑
# run_hive_cmd(tbls)

# 开启线程并发执行
for records in chunk(tbls, max_threads):
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
fail_record.close()