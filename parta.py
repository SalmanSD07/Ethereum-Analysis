import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("work")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            print('Its all in the mind')
            print(int(fields[11]))
            return True
        except:
            return False
    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    lines=spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = lines.filter(good_line)
    
    # fields = clean_lines.split(',')
    # raw_timestamp = int(fields[6])
    # tranCount = time.strftime('%Y-%m', time.gmtime(raw_timestamp))
    tranCount = clean_lines.map(lambda b: (time.strftime("%Y-%m",time.gmtime(int(b.split(',')[11]))),1))
    print(tranCount)
    tCount=tranCount.reduceByKey(operator.add)
    
    def add_tuple(a,b):
        return (a[0]+b[0],a[1]+b[1])
    
    trt=clean_lines.map(lambda b:(time.strftime("%Y-%m",time.gmtime(int(b.split(',')[11]))),(float(b.split(',')[7]),1)))
    trt_reduce=trt.reduceByKey(add_tuple).mapValues(lambda x:x[0]/x[1])
    
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'work' + date_time + '/out_part1.csv')
    my_result_object.put(Body=json.dumps(tCount.take(1000)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'work' + date_time + '/out_part2.csv')
    my_result_object.put(Body=json.dumps(trt_reduce.take(1000)))
    
    
    
    
    
    # lines.saveAsTextFile('out_partA')
    # df.to_csv('./out_partA.csv')
    print("Work has been completed")
    spark.stop()
    
    
    