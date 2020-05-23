import os
import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import argparse
from lable import lable
from location import location

if __name__ == "__main__":

    parser = argparse.ArgumentParser("Transform Data")
    parser.add_argument("--type", help="Type of Transformation")
    args = parser.parse_args()

    sc = SparkContext(appName="Transform Labels Data", pyFiles=["geolife_trajectory/transform/lable.py", "geolife_trajectory/transform/location.py"])
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2)
    messages = KafkaUtils.createDirectStream(ssc, ["test",], {"metadata.broker.list":"localhost:9092"})
    
    if args.type == "lable":  
        _lable = lable()
        all_labels = messages.map(lambda x : x[1])
        lables_dict = all_labels.flatMap(lambda lines : _lable.mappingLabels(lines.split("\n")))
        
        lables_dict.foreachRDD(lambda rdd : _lable.storedToRedis(rdd))
        ssc.start()
        ssc.awaitTermination()

    elif args.type == "location":
        _location = location()
        lines = messages.map(lambda x: x[1])
        summary_locations = lines.flatMap(lambda line: _location.get_start_end_location(line.split("\n")))   
        
        summary_locations.foreachRDD(lambda rdd : _location.storeToRedis(rdd)) 
        ssc.start()
        ssc.awaitTermination()

    