class Student(object):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "Student object (name: %s)" % self.name


student = Student("Nathan")
print(student)
print(student.__repr__)

class Fib(object):
    def __init__(self):
        self.a, self.b = 0, 1

    def __iter__(self):
        return self

    def __next__(self):
        self.a, self.b = self.b, self.a + self.b
        if self.a > 100:
            raise StopIteration()
        return self.a

    def __getitem__(self, item):
        a, b = 0, 1
        if isinstance(item, int):
            for x in range(item):
                a, b = b, a + b
            return a
        if isinstance(item, slice):
            start = item.start
            stop = item.stop
            if start is None:
                start = 0
            L = []
            for x in range(stop):
                if x >= start:
                    L.append(a)
                a, b = b, a+b
            return L

class Student(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, *args, **kwargs):
        print("call invoke")

for n in Fib():
    print(n)
f = Fib()
print(f[1])

print(list(range(10)[5:8]))
print(f[5:20])
s = Student("test")
print(s())


