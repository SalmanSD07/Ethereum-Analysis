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
        .appName("partd_1")\
        .getOrCreate()
    
    def good_line_transaction(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            
            float(fields[7])
            int(fields[11])
            str(fields[6])
            return True
        except:
            return False
        
    def good_line_scam(line):
        try:
            fields = line.split(',')
            if len(fields)!=8:
                return False
            str(fields[0])
            str(fields[4])
            str(fields[6])
            
            
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
    
    lines=spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")
    lines1=spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scams = lines.filter(good_line_scam)
    
    
    transactions = lines1.filter(good_line_transaction)
    
    scam = scams.map(lambda b: (str(b.split(',')[6]), (str(b.split(',')[4]),str(b.split(',')[0]))))

    # calculate Wei received at each address in each month
    tran = transactions.map(lambda b: (b.split(',')[6],(float(b.split(',')[7]),time.strftime('%Y-%m', time.gmtime(int(b.split(',')[11]))),1)))
    join = tran.join(scam)
   
    tscomb1 = join.map(lambda b: ((b[1][1][0],b[1][0][1]), (b[1][0][0], b[1][0][2])))
    tscomb2= join.map(lambda b: ( b[1][1][1], (b[1][0][0]/100000000000000000000, b[1][0][2])))
    out1 = tscomb1.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
    out1 = out1.map(lambda b: (b[0][1],(b[0][0],b[1][0]/100000000000000000000,b[1][1])))
    out2 = tscomb2.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
    
    # jStd=out2.takeOrdered(1,key=lambda x: -x[1])
    jStd=out1.take(10000)
    
    
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'partd_1' + date_time + '/out_partD_1.txt')
    my_result_object.put(Body=json.dumps(jStd))
    my_result_object = my_bucket_resource.Object(s3_bucket,'partd_1' + date_time + '/out_partD_2.txt')
    my_result_object.put(Body=json.dumps(out2.take(10000)))
    
    print("Work has been completed")
    spark.stop()
    
    
    
    
    
    
    