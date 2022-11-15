import time

print("hi")

with open("/mnt/data/hi.txt", "r") as f:
    print(f.read())

with open("/mnt/data/amogus.txt", "r") as f:
    print(f.read())

# with open("/mnt/data/other1.txt", "r") as f:
#     print(f.read())

with open("/mnt/data/other2.txt", "r") as f:
    print(f.read())

f = open("/usr/src/ds/functions/example_one.py", "r")
print(f.read())

for i in range(5):
    print("hi")
    time.sleep(1)