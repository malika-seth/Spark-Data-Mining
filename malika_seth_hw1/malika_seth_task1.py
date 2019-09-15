from pyspark.context import SparkContext
from collections import OrderedDict         #preserve the order of json file output
import sys
import json
import pyspark

file_path= sys.argv[1]
output_path= sys.argv[2]

sc= SparkContext("local[*]")
sc.setLogLevel("ERROR")

tf=sc.textFile(file_path)
data=tf.map(lambda x: json.loads(x)).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
data1=data.map(lambda x: (x["useful"],x["stars"],x["text"],x["user_id"],x["business_id"])).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)

RDD=data1.filter(lambda x: x[0]>0)              #TASK1A
useful=RDD.count()

RDD2=data1.filter(lambda x: x[1]==5.0)          #TASK1B
stars=RDD2.count()

RDD3=data1.max(lambda x: len(x[2]))             #TASK1C
maxchar=len(RDD3[2])

user=data1.map(lambda x: x[3]).distinct().count()   #TASK1D

topuser=data1.map(lambda x: (x[3], 1)).reduceByKey(lambda x, y: x + y).sortByKey(False).takeOrdered(20, key= lambda x: -x[1])        #TASK1E

business=data1.map(lambda x: x[4]).distinct().count()       #TASK1F

topbusiness=data1.map(lambda x: (x[4], 1)).reduceByKey(lambda x, y: x + y).sortByKey(False).takeOrdered(20, key= lambda x: -x[1])    #TASK1G

json_fileop={}
json_fileop['n_review_useful']=useful
json_fileop['n_review_5_star']=stars
json_fileop['n_characters']=maxchar
json_fileop['n_user']=user
json_fileop['top20_user']=topuser
json_fileop['n_business']=business
json_fileop['top20_business']=topbusiness
with open(output_path, "w") as f:
     json.dump(OrderedDict(json_fileop),f)
