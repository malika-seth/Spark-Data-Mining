from pyspark.context import SparkContext
from collections import OrderedDict         #preserve the order of json file output
import sys
import time
import json
import pyspark

file_path1=sys.argv[1]
file_path2=sys.argv[2]
output_path1=sys.argv[3]
output_path2=sys.argv[4]

sc= SparkContext("local[*]")
sc.setLogLevel("ERROR")
tf1=sc.textFile(file_path1)
tf2=sc.textFile(file_path2)

data1 =tf1.map(lambda x: (json.loads(x)["business_id"],json.loads(x)["stars"]))
data2 =tf2.map(lambda x: (json.loads(x)["business_id"],json.loads(x)["state"])).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)

statecount=data2.map(lambda x: x[1]).distinct().count()

RDD=data1.join(data2).map(lambda x: (x[1][1],x[1][0])).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
st=RDD.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
task1=st.mapValues(lambda x: x[0]/x[1]).sortByKey(False)
task1f=task1.top(statecount, key= lambda x: x[1])

t1_output_header='state,stars'
with open(output_path1, 'w') as file_op:
    file_op.write(t1_output_header)
    for item in task1f:
        file_op.write("\n%s" % str(item).replace("(", "").replace(")", "").replace("'", "").replace(" ",""))
    file_op.close()


t2=time.time()
t2a=task1.collect()
print("Task 2B m1:", sorted(t2a,key=lambda x: x[1], reverse=True)[:5])
t3=time.time()-t2


t0=time.time()
t2b=sc.parallelize(task1f).take(5)
print("Task 2B m2: ",t2b)
t1=time.time()-t0

json_fileop={}
json_fileop['m1']=t3
json_fileop['m2']=t1
json_fileop['explanation']="The collect() function takes more time as it brings all the elements of the dataset into the main driver memory or master node, due to which alot of time is consumed as the whole rdd is brought into the driver memory. The take() function takes less time as it just selects the first n items from the rdd"
with open(output_path2, "w") as f:
    out= json.dump(OrderedDict(json_fileop),f)
