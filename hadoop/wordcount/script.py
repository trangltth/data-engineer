# 1. apply hdfs
import pydoop.hdfs as hdfs

for line in hdfs.open("/test/test.txt"):
    print("data call from pydoop.hdfs function: ", line)

# 2. script method
# def mapper(key, value, writer):
#   for word in value.split():
#     writer.emit(word, "1")

# def reducer(key, value_list, writer):
#   writer.emit(key, sum(map(int, value_list)))