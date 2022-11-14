import time

print("hi")

f = open("/mnt/data/hi.txt", "r")
print(f.read())

for i in range(5):
    print("hi")
    time.sleep(1)