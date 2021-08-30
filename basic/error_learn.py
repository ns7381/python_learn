import logging
logging.basicConfig(level=logging.INFO)

try:
    print("try....")
    raise Exception
    r = 10 / int('s2')
    print('result:', r)
except ValueError as e:
    logging.exception("value error:", e)
except ZeroDivisionError as e:
    print("except:", e)
else:
    print("no error")
finally:
    print("finally....")
print("End")


class my_error(ValueError):
    pass