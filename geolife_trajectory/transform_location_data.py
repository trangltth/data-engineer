import sys
from pyspark.streaming.kafka import KafkaUtils, Broker
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import logging, math
from datetime import datetime
from pyspark.sql import SparkSession


def get_start_end_location(location_RDDs):
    temp_date = datetime.strptime("1899-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    max = [-1, temp_date]
    min = [-1, temp_date]
    list_day = []
    user_id = "0"
    time_period = "0"
    for i in range(len(location_RDDs)):
        data = location_RDDs[i].split(",")
        if len(data) > 1:
            user_id = data[0]
            time_period = data[1]
            current_time = str(data[-1]).strip()        
            current_date = str(data[len(data) - 2]).strip()
        if not ((current_time == "") | (current_date == "")):
            # current_time = "00:00:00" if (current_time == "") else current_time
            # current_date = "1899-01-01" if (current_date == "") else current_date
            datetime_str = datetime.strptime(current_date + " " + current_time, "%Y-%m-%d %H:%M:%S")
            if ((max[1] < datetime_str) | (max[0] == -1)):
                max[0] = i
                max[1] = datetime_str
            if ((min[1] > datetime_str) | (min[0] == -1)):
                min[1] = datetime_str
                min[0] = i
    
    duration = max[1] - min[1]
    location_start = location_RDDs[min[0]]
    location_end = location_RDDs[max[0]]
    distance = getDistance(location_start, location_end)

    information = { "id": user_id + "-" + time_period,
                    "user_id": user_id,
                    "time_period": time_period,
                    "duration": str(duration),
                    "location_start": location_start,
                    "location_end": location_end,
                    "distance": str(distance)}

    list_day.append(information)

    return list_day

def getDistance(location_start, location_end):
    # to understanding hafversine formular -> implement 
    r = 6371e73
    # theta1 = location_start[1] * math.pi / 180
    # theta2 = location_end[1] * math.pi / 180
    # delta_theta = (location_end[1] - location_start[1])
    # delta_gama = (location_end[2] - location_start[2])

    return r

def storeToRedis(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.appName("Geolife Trajectory").config("spark.redis.host", "localhost").config("spark.redis.port","6379").getOrCreate()
        df_data = spark.createDataFrame(rdd)
        df_data.show()
        df_data.write.format("org.apache.spark.sql.redis").option("table", "locations").option("key.column", "id").save(mode="append")

if __name__ == "__main__":

    # broker = Broker(host, port)
    sc  = SparkContext(appName="geolife_trajectory")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, batchDuration=2)

    messages = KafkaUtils.createDirectStream(ssc, ['test'], {"metadata.broker.list":"localhost:9092"})
    # messages = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer", {'test':1})

    lines = messages.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split("\n"))
    summary_locations = lines.flatMap(lambda line: get_start_end_location(line.split("\n")))   
    
    summary_locations.foreachRDD(storeToRedis) 

    ssc.start()
    ssc.awaitTermination()


