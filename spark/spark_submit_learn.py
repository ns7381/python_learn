# coding: utf-8
import subprocess
from datetime import datetime


def submit_job():
    cmd_str = '''
spark-submit --class com.compute.Kafka2HiveTool \
     --deploy-mode cluster --master yarn --executor-cores 4 \
     --num-executors 1 --executor-memory 8g \
     --conf spark.yarn.maxAppAttempts=1 \
     --conf spark.streaming.concurrentJobs=2 \
     --conf spark.streaming.kafka.maxRatePerPartition=100000 \
     --conf spark.streaming.backpressure.enabled=true \
     --conf spark.km.jdq.batch.interval.second=30 \
     --conf spark.dynamicAllocation.enabled=false \
     --conf spark.session.timeout=-1 \
     --conf spark.session.timeout.enable=false \
     kafka2hive-1.0-SNAPSHOT-shaded.jar
'''
    p = subprocess.call(cmd_str, shell=True)

    print('返回值是:' + str(p))

    if p == 0:
        print(str(datetime.now()) + ': 任务运行成功')
        return True
    else:
        print(str(datetime.now()) + ": 任务运行失败")
        return False


def run_with_retry():
    count = 0
    while True:
        count = count + 1
        print(str(datetime.now()) + ": 第" + count + "次运行")
        submit_job()


if __name__ == '__main__':
    run_with_retry()
