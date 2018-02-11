from io import StringIO

f = StringIO("hello\nnathan\nhaha!")
# f.write("hello\nnathan\nhaha!")

while True:
    s = f.readline()
    if s == '':
        break
    print(s.strip())