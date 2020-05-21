from pyspark import SparkContext
from datetime import datetime
import re, logging
from operator import add

def main(): 
    date_suffix = re.sub("[\:,\-,\ ]*", "", str(datetime.now()))

    sc = SparkContext(appName='SparkCount')
    data = sc.textFile("/user/trang/input/test.txt")

    lines = data.flatMap(lambda line: line.split()).map(lambda word: (word,1)).reduceByKey(add).collect()
    lines_sdd = sc.parallelize(lines)

    lines_sdd.saveAsTextFile("/user/trang/output/" + date_suffix)
    sc.stop()

if __name__ == "__main__":
    main()