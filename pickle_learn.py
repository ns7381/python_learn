import pickle

d = dict(name="nathan", age=27)
# f = open("pickle.txt", "wb")
# print(pickle.dump(d, f))
f = open("pickle.txt", "rb")
d = pickle.load(f)
f.close()
print(d)