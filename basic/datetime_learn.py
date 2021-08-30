from datetime import datetime
print(datetime.now().timestamp())
print(datetime(2019,12,3,23))

if __name__ == '__main__':
    print(datetime.now())
    import time


    start = time.time()
    end = time.time()
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    from datetime import date, timedelta

    today = date.today()
    yesterday = today - timedelta(days=1)
    print(today)
    print(yesterday)

