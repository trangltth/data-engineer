from pyspark import SparkContext
import sys, re
import logging

def log_info(_message):
    log = logging.getLogger(name='text_search')
    log.setLevel('DEBUG')

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(message)s")

    handler.setFormatter(formatter)

    log.addHandler(handler)

    log.debug(_message)

def main():
    sc = SparkContext(appName='Text Search')

    request_text = 'this'

    log_info(request_text)
    data_file = sc.textFile('/user/trang/input/test.txt')

    word_summary = data_file.flatMap(lambda line: line.split()).map(lambda word: (word,1))

    data_filter = word_summary.reduceByKey(lambda key, counts : (request_text == key))

    result = data_filter.glom().collect()

    log_info(result)

    sc.stop()

if __name__ == "__main__":
    main()
