from pyspark.context import SparkContext
import collections
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import sys
import pyspark
import time
import math

def calcitem_sim(testactive,buss):
    userintersect=[]
    try:
        userintersect = list(bus_userDict[testactive].intersection(bus_userDict[buss]))
    except:
        pass
    if not userintersect:
        return 0.2
    avg_testactive = sum([userbus_ratingDict[(users,testactive)] for users in userintersect]) / len(userintersect)
    avg_newbus = sum([userbus_ratingDict[(users,buss)] for users in userintersect]) / len(userintersect)
    num = sum([(userbus_ratingDict[(users,testactive)] - avg_testactive) * (userbus_ratingDict[(users,buss)] - avg_newbus) for users in userintersect])
    den = math.sqrt(sum([(userbus_ratingDict[(users,testactive)] - avg_testactive) **2 for users in userintersect])) * math.sqrt(sum([(userbus_ratingDict[(users,buss)] - avg_newbus) **2 for users in userintersect]))
    if den == 0.0:
        return 0.5
    return float(num/den)


def calcuser_sim(testactive,users):
    busintersect=[]
    try:
        busintersect = list(user_busDict[testactive].intersection(user_busDict[users]))
    except:
        pass
    if not busintersect:
        return 0.0, 0.0
    avg_testactive = sum([userbus_ratingDict[(testactive,business)] for business in busintersect]) / len(busintersect)
    avg_newuser = sum([userbus_ratingDict[(users,business)] for business in busintersect]) / len(busintersect)
    num = sum([(userbus_ratingDict[(testactive,business)] - avg_testactive) * (userbus_ratingDict[(users,business)] - avg_newuser) for business in busintersect])
    den = math.sqrt(sum([(userbus_ratingDict[(testactive,business)] - avg_testactive) **2 for business in busintersect])) * math.sqrt(sum([(userbus_ratingDict[(users,business)] - avg_newuser) **2 for business in busintersect]))
    if den == 0.0:
        return 0.5, float(avg_newuser)
    return float(num/den), float(avg_newuser)

def clip(p):

    if p > 5.0 :
        pr = 5.0
    elif p < 0.0 :
        pr = 0.0
    else :
        pr = p
    return float(pr)


def calcuser_pred(x,y):
    testuser = x
    testbus = y
    num_sum = 0.0
    den_sum = 0.0
    #print ("yes1111")
    allusers = []
    try:
        allusers = list(bus_userDict[testbus])
        allbus = list(user_busDict[testuser])

    except:
        fin_pred = 3.0
        return float(fin_pred)

    for user in allusers:
        if (user != testuser):
            similarity_weight, useravg = calcuser_sim(testuser,user)
            num_sum = num_sum + ((userbus_ratingDict[(user,testbus)] - useravg) * similarity_weight)
            den_sum = den_sum + abs(similarity_weight)

    if den_sum != 0.0:
        div = num_sum/den_sum
        pred = user_avgratingsDict[testuser] + div
    else:
        pred = user_avgratingsDict[testuser]

    fin_pred = clip(pred)
    #return float(round(fin_pred,3))
    return float(fin_pred)

def calcitem_pred(x,y):
    testuser = x
    testbus = y
    num_sum = 0.0
    den_sum = 0.0
    allbus = []
    try:
        allbus = list(user_busDict[testuser])
        allusers = list(bus_userDict[testbus])
    except:
        fin_pred = 3.2
        return float(fin_pred)
    neighbordict = {}
    for bus in allbus:
        if (bus != testbus):
            similarity_weight = calcitem_sim(testbus,bus)
            neighbordict[bus] = float(similarity_weight)

    sorted_dict = sorted(neighbordict.items(), key=lambda x: x[1], reverse= True)
    limit= int(len(sorted_dict)/2)

    nbus_dict = collections.OrderedDict(sorted_dict[:limit])
    for k,v in nbus_dict.items():
        num_sum = num_sum + (userbus_ratingDict[(testuser,k)] * v)
        den_sum = den_sum + abs(v)

    if den_sum != 0.0:
        div = num_sum / den_sum
        pred = div
    else:
        pred = 0.0

    fin_pred = clip(pred)
    return float(fin_pred)







if __name__ == "__main__":


    file_path = sys.argv[1]
    ground_path = sys.argv[2]
    case_id = sys.argv[3]
    output_path = sys.argv[4]
    sc= SparkContext("local[*]")
    sc.setLogLevel("ERROR")
    start= time.time()
    lines = sc.textFile(file_path)
    header = lines.first()
    inputRDD= lines.filter(lambda x: x != header).map(lambda x: tuple(x.split(","))).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)

    testlines = sc.textFile(ground_path)
    testheader = testlines.first()
    test_inputRDD = testlines.filter(lambda x: x != testheader).map(lambda x: x.split(",")).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    if case_id == "1":
        # Keeping user dict
        userRDD = inputRDD.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x).sortByKey()
        user = userRDD.map(lambda x: x[0]).collect()
        cu = len(user)
        userdict = {}
        for i in range(0, cu):
            userdict[user[i]] = int(i)

        # Keeping business dict
        busRDD = inputRDD.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x).sortByKey()
        bus = busRDD.map(lambda x: x[0]).collect()
        bu = len(bus)
        busdict = {}
        for i in range(0, bu):
            busdict[bus[i]] = int(i)

        trainRDD = inputRDD.map(lambda x: Rating(userdict[x[0]], busdict[x[1]], float(x[2])))
        rank = 2
        numIterations = 7

        model = ALS.train(trainRDD, rank, numIterations, 0.5)

        finalground = test_inputRDD.map(lambda x: (x[0], x[1])).collect()
        truefinal = {}
        coldstartground = {}
        for item in finalground:
            if (item[0] not in userdict) or (item[1] not in busdict):
                coldstartground[(item[0], item[1])] = float(3.0)
            else:
                truefinal[(userdict[item[0]], busdict[item[1]])] = 0.0

        coldstartrdd = sc.parallelize(list(coldstartground.items()))
        coldstartmap = coldstartrdd.map(lambda x: ((x[0][0], x[0][1]), x[1]))

        truefinalrdd = sc.parallelize(list(truefinal.items()))
        truefinalmap = truefinalrdd.map(lambda x: (x[0][0], x[0][1]))

        revuser = dict(map(reversed, userdict.items()))
        revbus = dict(map(reversed, busdict.items()))

        predictions = model.predictAll(truefinalmap).map(lambda r: ((revuser[r[0]], revbus[r[1]]), r[2]))
        preds = predictions.union(coldstartmap)
        finalpredground = test_inputRDD.map(lambda x: ((x[0], x[1]), float(x[2])))
        ratesAndPreds = finalpredground.join(preds)

        MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
        RMSE = pow(MSE, 0.5)
        print("Root Mean Squared Error = " + str(RMSE))

        to_print = preds.map(lambda x: (x[0][0], x[0][1], x[1])).collect()

        with open(output_path, 'w') as f:
            headers = 'user_id, business_id, prediction'
            f.write(headers)
            for item in to_print:
                f.write("\n%s" % str(item).replace("(", "").replace(")", "").replace("'", "").replace(" ", ""))
            f.close()

        end = time.time()
        #print("Duration ", end - start)


    elif case_id == "2" or case_id == "3" :
        userbus_rating = inputRDD.map(lambda x: ((x[0],x[1]), float(x[2])))
        l = userbus_rating.collect()
        userbus_ratingDict = dict(l)
        #print(userbus_ratingDict)

        user_bus = inputRDD.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).mapValues(set)
        u = user_bus.collect()
        user_busDict = dict(u)
        #print(user_busDict)

        bus_user = inputRDD.map(lambda x: (x[1],[x[0]])).reduceByKey(lambda x,y: x+y).mapValues(set)
        ub = bus_user.collect()
        bus_userDict = dict(ub)

        user_avgratings = inputRDD.map(lambda x: (x[0],[float(x[2])])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1])))
        uavg = user_avgratings.collect()
        user_avgratingsDict = dict(uavg)

        if case_id == "2":

            findpred = test_inputRDD.map(lambda x: ((x[0],x[1]),calcuser_pred(x[0],x[1])))
            to_print = findpred.collect()

            checktestRDD = test_inputRDD.map(lambda x: ((x[0],x[1]), float(x[2])))

            ratesAndPreds = checktestRDD.join(findpred)

            MSE = ratesAndPreds.map(lambda r: (abs(r[1][0] - r[1][1]))**2).mean()
            RMSE = pow(MSE, 0.5)
            print("Root Mean Squared Error = " + str(RMSE))

            with open(output_path, 'w') as f:
                headers = 'user_id, business_id, prediction'
                f.write(headers)
                for item in to_print:
                    f.write("\n%s" % str(item).replace("(","").replace(")","").replace("'","").replace(" ",""))
                f.close()
            end= time.time()
            #print("Duration: ",end-start)

        elif case_id == "3":

            findpred = test_inputRDD.map(lambda x: ((x[0], x[1]), calcitem_pred(x[0], x[1])))
            to_print = findpred.collect()

            checktestRDD = test_inputRDD.map(lambda x: ((x[0], x[1]), float(x[2])))

            ratesAndPreds = checktestRDD.join(findpred)

            MSE = ratesAndPreds.map(lambda r: (abs(r[1][0] - r[1][1])) ** 2).mean()
            RMSE = pow(MSE, 0.5)
            print("Root Mean Squared Error = " + str(RMSE))

            with open(output_path, 'w') as f:
                headers = 'user_id, business_id, prediction'
                f.write(headers)
                for item in to_print:
                    f.write("\n%s" % str(item).replace("(","").replace(")","").replace("'","").replace(" ",""))
                f.close()
            end = time.time()
            #print("Duration: ", end - start)
