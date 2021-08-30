class student(object):
    def get_age(self):
        return 12


s = student()
print(type(s))


def hello(self, name):
    print("hello %s" % name)


t = type("student", (object,), dict(hello=hello))
print(t)
