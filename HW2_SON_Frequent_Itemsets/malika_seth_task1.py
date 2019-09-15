from pyspark.context import SparkContext
import itertools
from collections import Counter
from operator import add
import os
import sys
import time
import pyspark

def gencomb(freqitem,lenfreqitem):
    cand= []
    for x in range(0,lenfreqitem-1):
         first = freqitem[x]
         firstelem = sorted(first)
         for y in range(x+1,lenfreqitem):
              second = freqitem[y]
              secelem= sorted(second)
              newItem = []
              if firstelem[0:len(firstelem)-1] ==  secelem[0:len(secelem)-1] :
                   newItem=list(firstelem)
                   newItem.append(secelem[len(secelem)-1])
                   newItem.sort()
                   cand.append((newItem))
    #print("candidate ",cand)
    return cand



def checksub(checksub,prev,len,initbasket,ktuple):
     store = Counter()
     candidates = []
     for k in checksub:
          item = set(k)
          count = 0
          for x in prev:
              if set(x).issubset(item):
                    count +=1
              if count == ktuple:
                    candidates.append(tuple(k))
                    # print("candidates: ",candidates)
                    break

     candidates = list(dict.fromkeys(candidates))
     for basket in initbasket:
         bas= set(basket)
         for candidate in candidates:
             if set(candidate).issubset(bas):
                 store[tuple(candidate)] += 1
     return store


def prune(checksubs,threshold):
     freq =[]
     for x in checksubs:
          if checksubs[x] >= threshold:
               freq.append(tuple(x))
     return sorted(freq)



def apriori(basket,basket_count,sup):

    initbasket = list(basket)
    support = float(sup)
    partition_baskets= len(initbasket)
    #print("basket ",partition_baskets)
    total_baskets = basket_count
    threshold = support * (float(partition_baskets)/float(total_baskets))
    #print("THRESHOLD ",threshold)
    final_itemsets = []
    single_count={}
    freqsingle=[]
    single_print=[]
#SINGLTON
    for bas in initbasket:
        for item in bas:
             if item in single_count:
                single_count[item] += 1
             else:
                single_count[item] = 1

    for x in single_count:
         if single_count[x] >= threshold:
             freqsingle.append(x)
             single_print.append((x,))
    single = sorted(tuple(freqsingle))
    single_print= sorted(tuple(single_print))
    final_itemsets=single
#PAIRS

    res = list(itertools.combinations(single, 2))
    paircount = Counter()
    freqpair = []
    cand = []

    for item in res:
         count =0
         for s in single_count:
            if s in single:
                count +=1
            if count >=2:
                cand.append(tuple(item))
                break

    cand = list(dict.fromkeys(cand))

    for basket in initbasket:
        bas =set(basket)
        for candidate in cand:
            if candidate in basket:
                paircount[tuple(candidate)] += 1
            else:
                temp = list(candidate)
                if set(temp).issubset(bas):
                    paircount[tuple(candidate)] += 1

    for x in paircount:
         if paircount[x] >= threshold:
              freqpair.append(tuple(sorted(x)))


    pair=sorted(freqpair)
    pair = list(dict.fromkeys(pair))
    #print("pair   ",len(pair))
    final_itemsets = final_itemsets + pair
    single_print = single_print + pair
    #print("pair   ",single_print)
    frequent=pair
    k=3
    while frequent != []:

        gencombination= gencomb(frequent,len(frequent))
        #print(k," itemsets",len(gencombination))
        checksubsets = checksub(gencombination,frequent,len(gencombination),initbasket,k)
        #print("candidate ", len(checksubsets))
        freqitemsets= prune(checksubsets,threshold)
        #print(k, " -tuple freqitem ", len(freqitemsets))
        frequent=list(freqitemsets)
        if frequent != []:
            final_itemsets = final_itemsets + frequent
            single_print = single_print + frequent
        k +=1
    #print("Final itemsets ", single_print)

    yield (single_print)

def countmap2(bt):
    initbasket=list(bt)
    count = Counter()

    for basket in initbasket:
        bas=set(basket)
        #counter =0
        for item in val:
            if item in bas:
                count[item] += 1
            else:
                temp = list(item)
                if set(temp).issubset(bas):
                    count[tuple(item)] += 1
    #print("countint ",list(count.items()))

    yield (list(count.items()))



if __name__ == "__main__":

    case = sys.argv[1]
    support = sys.argv[2]
    input_path = sys.argv[3]
    output_path = sys.argv[4]

    sc= SparkContext("local[*]")
    sc.setLogLevel("ERROR")
    start = time.time()
    lines = sc.textFile(input_path)
    header = lines.first()
    RDD= lines.filter(lambda x: x != header).map(lambda x: tuple(x.split(",")))

    if case == "1":
        basket1= RDD.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x + y)

    elif case == "2":
        basket1= RDD.map(lambda x: (x[1],[x[0]])).reduceByKey(lambda x,y: x + y)


    bt=basket1.values()
    basket_count = bt.count()

    mapbasket1 = bt.mapPartitions(lambda x: apriori(x,basket_count,support)).flatMap(lambda x: x)
    phase1_map= mapbasket1.map(lambda itemsets: (itemsets,1))

    phase1_reduce= phase1_map.reduceByKey(lambda x,y : x).sortBy(keyfunc= lambda x: (len(x[0]),x[0]))
    val= phase1_reduce.keys().collect()
    #print("phase1reduce ", len(phase1_reduce.collect()))
    with open(output_path,'w') as fileop:
        fileop.write("Candidates: \n")

        old = 1
        storestr = ""
        for index in range(len(val)):
            element = val[index]

            if index == len(val) - 1:
                fileop.write(storestr.replace(")(", "),("))
                if len(element) > old:
                    fileop.write("\n\n")
                    fileop.write(str(element))
                    break
                else:
                    fileop.write(","+str(element))
                    break

            if len(element) > old:
                fileop.write(storestr.replace(")(","),("))
                fileop.write("\n\n")
                old += 1
                storestr= ""
                fileop.write(str(element))
            else:
                if len(element) == 1:
                    storestr += str(element).replace(",)", ")")
                else:
                    storestr += "," + str(element)
        fileop.close()



    phase2_map = bt.mapPartitions(countmap2).flatMap(lambda x: x).reduceByKey(add).sortBy(keyfunc= lambda x: (len(x[0]),x[0]))

    phase2_reduce = phase2_map.filter(lambda x: x[1] >= float(support)).map(lambda x: x[0])
    res = phase2_reduce.collect()
    #print("res ", len(res))

    with open(output_path,'a') as fileop:
        fileop.write("\n\nFrequent Itemsets: \n")

        old = 1
        storestr = ""
        for index in range(len(res)):
            element = res[index]

            if index == len(res) - 1:
                fileop.write(storestr.replace(")(", "),("))
                if len(element) > old:
                    fileop.write("\n\n")
                    fileop.write(str(element))
                    break
                else:
                    fileop.write(","+str(element))
                    break

            if len(element) > old:
                fileop.write(storestr.replace(")(","),("))
                fileop.write("\n\n")
                old += 1
                storestr= ""
                fileop.write(str(element))
            else:
                if len(element) == 1:
                    storestr += str(element).replace(",)", ")")
                else:
                    storestr += "," + str(element)
        fileop.close()

    end = time.time()
    print("Duration:", end - start)

