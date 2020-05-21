import os
import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

# sample [
#           {   user_id: 1, 
#               label: [ 
#                       {   start_time: 1900-01-01, 
#                           end-time: 1900-01-01, 
#                           transportation_mode: "walk"
#                       }
#                      ]
#            }
#         ]
def mappingLabels(lable_RDDs):
    lable_general=[]
    
    is_next_user = 0
    for label in lable_RDDs:
        # check empty line
        label = str(label)
        lable_user={}
        lable_info = {}
        if (label.strip() == ""):
            continue
        lable_context = label.split(",")
        is_lable_general_update = 0
        
        lable_user['user_id'] = lable_context[0]
        lable_user['label'] = []
        lable_info['start_time'] = lable_context[1]
        lable_info['end_time'] = lable_context[2]
        lable_info['transportation_mode'] = lable_context[3]
        lable_user['label'].append(lable_info)
        
        for idx, _user in enumerate(lable_general):
            if _user['user_id'] == lable_context[0]:
                is_lable_general_update = 1
                lable_general[idx]['label'].append(lable_info)
                break
            
        if not (is_lable_general_update):
            lable_general.append(lable_user)

    return enumerate(lable_general)

def storedToRedis(lable_RDDs):
    if not lable_RDDs.isEmpty():
        try:
            spark = SparkSession.builder.appName("storing data to Redis").config("spark.redis.host", "localhost").config("spark.redis.port","6379").getOrCreate()
            _schema = ["id", "content"]
            
            df_location = spark.createDataFrame(lable_RDDs, schema=_schema)        
            df_location.show()
            df_location.write.format("org.apache.spark.sql.redis").option("table", "lables").option("key.column", "id").save(mode="append")
        except Exception as ERROR:
            logging.error(ERROR)
    

if __name__ == "__main__":
    
    sc = SparkContext(appName="Transform Labels Data")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2)
    messages = KafkaUtils.createDirectStream(ssc, ["test",], {"metadata.broker.list":"localhost:9092"})
    
    all_labels = messages.map(lambda x : x[1])
    lables_dict = all_labels.flatMap(lambda lines : mappingLabels(lines.split("\n")))
    
    lables_dict.foreachRDD(storedToRedis)

    lables_dict.pprint()

    ssc.start()
    ssc.awaitTermination()