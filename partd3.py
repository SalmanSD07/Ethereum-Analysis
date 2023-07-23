import sys, string
import os
import socket
import time
import operator
import boto3
import json
import pandas as pd                                                            
from pyspark.sql import SparkSession
from datetime import datetime
                                                           
                                               

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ether")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            str(fields[4])
            str(fields[5])
            str(fields[6])
            str(fields[7])
            str(fields[8])
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
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

    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_lines = lines.filter(good_line)
   
   
    out = lines.map(lambda b: ('Memory',(len(str(b.split(',')[4]))-2,len(str(b.split(',')[5]))-2,len(str(b.split(',')[6]))-2,len(str(b.split(',')[7]))-2,len(str(b.split(',')[8]))-2)))
    out = out.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2],a[3]+b[3],a[4]+b[4]))

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Ether' + date_time + '/block.csv')
    my_result_object.put(Body=json.dumps(out.take(200)))
    
    print("Work has been completed")
   

    spark.stop()
