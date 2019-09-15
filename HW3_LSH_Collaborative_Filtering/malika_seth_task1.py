from pyspark import SparkConf
from pyspark.context import SparkContext
from itertools import combinations
from random import seed
from collections import Counter
import random
import os
import pyspark
import time
import sys

def hashfunc(row, m):
    numhash=150
    global var1, var2
    user_sig=[]
    for num in range(numhash):
        hash_min = float('Inf')
        a=var1[num]
        b=var2[num]
        for user in row:
            user_hash= ((a * user + b) % m)
            hash_min = min(hash_min,user_hash)
        user_sig.append(int(hash_min))
    return (user_sig)

def cand_pairs(x):
    paircand=[]
    pairband=list(x)
    for i in range(len(pairband)):
        for j in range(i+1,len(pairband)):
            paircand.append((pairband[i],pairband[j]))
    return (paircand)

def calc_jacc(x):
    candjacc=sorted(tuple(x))
    vec1= set(ubcombination[busdict.get(candjacc[0])][1])
    vec2= set(ubcombination[busdict.get(candjacc[1])][1])
    val_jacc= float(len(vec1 & vec2)/len(vec1 | vec2))
    #candjacc=sorted(candjacc)
    return (candjacc[0],candjacc[1],val_jacc)


if __name__ == "__main__":


    # script_dir = os.path.dirname(__file__)
    # file_path = os.path.join(script_dir, "/Users/nikhilmehta/Downloads/yelp_train.csv")
    #ground_path= os.path.join(script_dir, "/Users/nikhilmehta/Downloads/pure_jaccard_similarity.csv")
    file_path= sys.argv[1]
    output_path = sys.argv[2]
    sc= SparkContext("local[*]")
    sc.setLogLevel("ERROR")
    sf = SparkConf()
    sf.set("spark.executor.memory", "4g")
    start= time.time()
    lines = sc.textFile(file_path)
    header = lines.first()
    RDD= lines.filter(lambda x: x != header).map(lambda x: tuple(x.split(","))).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    userRDD= RDD.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x).sortByKey()
    user=userRDD.map(lambda x: x[0]).collect()
    cu = len(user)
    userdict ={}
    for i in range(0,cu):
        userdict[user[i]] = i
    #print(len(userdict))
    #print("users", len(user.collect()))
    busRDD= RDD.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x).sortByKey()
    bus=busRDD.map(lambda x: x[0]).collect()
    bu= len(bus)
    busdict ={}
    for i in range(0,bu):
        busdict[bus[i]] = i


    ubRDD = RDD.map(lambda x: (x[1],[int(userdict.get(x[0]))])).reduceByKey(lambda x,y: x+y).sortByKey()
    ubcombination = ubRDD.collect()
    seed(1)
    t1 =[]
    for i in range(150):
        t1.append(random.randint(1,cu))
    var1= list(t1)

    seed(2)
    t2 =[]
    for i in range(150):
        t2.append(random.randint(1,cu))
    var2= list(t2)

    ubsigRDD= ubRDD.map(lambda x: (x[0], hashfunc(x[1], cu))).collect()
    n=150 #no of hash functions
    b=50
    r= int(n/b)
    #print("sig ",ubsigRDD[:3])
    pairsGen =[]
    for band in range(b):
        hash_sig={}
        for comb in range(len(ubsigRDD)):
            user_sigHash= (tuple(ubsigRDD[comb][1][band*r: (band+1)*r]))
            #print("user ", user_sigHash)
            business_name= ubsigRDD[comb][0]
            if hash_sig.get(user_sigHash) :
                hash_sig[user_sigHash].append(business_name)
            else:
                hash_sig[user_sigHash]=[business_name]

        #print("hash", hash_sig.items())
        for key,value in hash_sig.items():
            if len(value) >1:
                 #print("yes")
                 pairsGen.extend(list(combinations(value,2)))

    #print("pairs ",pairsGen[:10])
    pairRDD= sc.parallelize(pairsGen)
    jacc_op = pairRDD.map(calc_jacc).filter(lambda x: float(x[2]) >= 0.5).distinct().sortBy(lambda x: x[0])
    #print("pairs ",len(jacc_op.collect()))
    to_print= jacc_op.map(lambda x: list(x)).collect()
    #print("pairs to print ",to_print[:5])

    # g = sc.textFile(ground_path)
    # header = g.first()
    # groundRDD= g.filter(lambda x: x != header).map(lambda x: tuple(x.split(","))).map(lambda x: (x[0],x[1]))
    # res_jacc=jacc_op.map(lambda x: (x[0],x[1]))
    # tp= len(res_jacc.intersection(groundRDD).collect())
    # fp= len(res_jacc.subtract(groundRDD).collect())
    # fn= len(groundRDD.subtract(res_jacc).collect())
    # print("fn,",fn)
    #
    #
    # precision = float(tp/(tp+fp))
    # print("prec",precision)
    #
    # recal= float(tp/(tp+fn))
    # print("recall",recal)

    with open(output_path, 'w') as f:
        headers = 'business_id_1, business_id_2, similarity'
        f.write(headers)
        for item in to_print:
            f.write("\n%s" % str(item).replace("[","").replace("]","").replace("'","").replace(" ",""))
        f.close()

    end= time.time()
    print("Duration ", end-start)






