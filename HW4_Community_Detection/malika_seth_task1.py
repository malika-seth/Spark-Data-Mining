import sys

from pyspark.context import SparkContext
from itertools import combinations
from collections import OrderedDict
import os
import pyspark
import time


# def getCommbfs(pairs):
#     dict_pair = dict(pairs)
#     bfs_q =[]
#     visited = {}
#     part_comm = []
#     comm = []
#     for i in dict_pair:
#         visited[i] = 0
#     for k in dict_pair:
#         bfs_q = []
#         bfs_q.append(k)
#         count = 0
#         while count < len(bfs_q):
#             parent_node = bfs_q[count]
#             if visited[parent_node] == 1:
#                 continue
#             else:
#                 visited[parent_node] = 1
#                 child_nodes = dict_pair[parent_node]
#                 for child in child_nodes:
#                     if visited[child] != 1:
#                         part_comm.append(child)
#                         bfs_q.append(child)
#             count +=1
#         comm.append(list(part_comm))
#     return comm



def newbtw(dictionary):
    graph_dict = dict(dictionary)
    community = set()
    newbetween_dict = {}
    for k in graph_dict:
        root = k
        level_dict = {root: 0}
        shortest_dict = {root: 1}
        bfs_queue=[]
        bfs_queue.append(root)
        child_parentdict={}
        allnodes= set(bfs_queue)
        count =0
        while count < len(bfs_queue):
            parent_top = bfs_queue[count]
            child_nodes = graph_dict[parent_top]
            for child in child_nodes:
                #print(child)
                if child not in allnodes:
                    allnodes.add(child)
                    bfs_queue.append(child)
                    level_dict[child] = level_dict[parent_top] + 1
                    child_parentdict[child] = [parent_top]
                    shortest_dict[child] = shortest_dict[parent_top]
                else:
                    if level_dict[child] == level_dict[parent_top] + 1:
                        child_parentdict[child] += [parent_top]
                        shortest_dict[child] += shortest_dict[parent_top]
            count += 1
        community.add(tuple(sorted(allnodes)))
        weights ={}
        for bfsitem in range(len(bfs_queue) -1,-1,-1):
            bfsnode = bfs_queue[bfsitem]
            if bfsnode not in weights:
                weights[bfsnode]= 1

            if bfsnode in child_parentdict:
                parents = child_parentdict[bfsnode]
                len_parents = len(parents)
                den_sum=0
                for item in parents:
                    den_sum += shortest_dict[item]

                for p in parents:
                    edge_weight = 0
                    if len_parents == 1:
                        edge_weight = float(weights[bfsnode])
                    elif len_parents >=2:
                        edge_weight = float(weights[bfsnode]) * (float(shortest_dict[p]) / den_sum)

                    if p not in weights:
                        weights[p] =1
                    weights[p] += edge_weight
                    edge =[]
                    edge.append(bfsnode)
                    edge.append(p)
                    arrange = tuple(sorted(edge))
                    if arrange not in newbetween_dict:
                        newbetween_dict[arrange] = float(edge_weight)
                    else:
                        newbetween_dict[arrange] += float(edge_weight)
    return newbetween_dict, community






def getModularity(original_dict,comm_list,edgecount):
    comm_pairs =[]
    term = 0
    for comm in comm_list:
        comm_pairs=[]
        comm_pairs.extend(list(combinations(comm,2)))
        for item in comm_pairs:
            degree_i=len(original_dict[item[0]])
            degree_j=len(original_dict[item[1]])
            if item[1] in original_dict[item[0]] and item[0] in original_dict[item[1]]:
                A = 1
            else:
                A = 0
            degree = float(degree_i * degree_j)
            term += float(A - ((degree * 0.5) / edgecount))

    modularity = float((0.5*term)/edgecount)
    return modularity




def bfsTree(root_node):
    root = root_node
    level_dict = {root: 0}
    shortest_dict = {root: 1}
    bfs_queue=[]
    bfs_queue.append(root)
    child_parentdict={}
    allnodes= set(bfs_queue)
    count =0
    while count < len(bfs_queue):
        parent_top = bfs_queue[count]
        child_nodes = pair_dict[parent_top]
        for child in child_nodes:
            #print(child)
            if child not in allnodes:
                allnodes.add(child)
                bfs_queue.append(child)
                level_dict[child] = level_dict[parent_top] + 1
                child_parentdict[child] = [parent_top]
                shortest_dict[child] = shortest_dict[parent_top]
            else:
                if level_dict[child] == level_dict[parent_top] + 1:
                    child_parentdict[child] += [parent_top]
                    shortest_dict[child] += shortest_dict[parent_top]
            #print(level_dict)
        count += 1
    #print(level_dict)
    #print("shortest_dict: ", shortest_dict)
    weights ={}
    for bfsitem in range(len(bfs_queue) -1,-1,-1):
        bfsnode = bfs_queue[bfsitem]
        if bfsnode not in weights:
            weights[bfsnode]= 1

        if bfsnode in child_parentdict:
            parents = child_parentdict[bfsnode]
            len_parents = len(parents)
            den_sum=0
            for item in parents:
                den_sum += shortest_dict[item]

            for p in parents:
                edge_weight = 0
                if len_parents == 1:
                    edge_weight = float(weights[bfsnode])
                elif len_parents >=2:
                    edge_weight = float(weights[bfsnode]) * (float(shortest_dict[p]) / den_sum)

                if p not in weights:
                    weights[p] =1
                weights[p] += edge_weight
                edge =[]
                edge.append(bfsnode)
                edge.append(p)
                arrange = sorted(edge)
                yield ((arrange[0],arrange[1]),float(edge_weight))

if __name__ == "__main__":

    # script_dir = os.path.dirname(__file__)
    # file_path = os.path.join(script_dir, "/Users/nikhilmehta/Downloads/sample_data.csv")
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    between_output_path = sys.argv[3]
    comm_output_path = sys.argv[4]
    sc= SparkContext("local[*]")
    sc.setLogLevel("ERROR")
    start= time.time()
    lines = sc.textFile(input_file_path)
    header = lines.first()
    RDD= lines.filter(lambda x: x != header).map(lambda x: tuple(x.split(","))).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    userRDD= RDD.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y).mapValues(set)
    u = userRDD.collect()
    user_busDict = dict(u)

    new = {}
    for k1,v1 in user_busDict.items():
        for k2, v2 in user_busDict.items():
            if k1 != k2:
                new[(k1,k2)] = v1.intersection(v2)

    p={}
    for k,v in new.items():
        if len(v)>=filter_threshold:
            p[k] = v

    pairs = list(p)
    pair = sc.parallelize(pairs)
    pairRDD = pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y)
    # print(pairRDD.collect())
    # print(len(pairRDD.collect()))
    pair_dict ={}
    for p in pairRDD.collect():
        pair_dict[p[0]]= p[1]

    bet = pairRDD.flatMap(lambda x: bfsTree(x[0])).reduceByKey(lambda x,y: x+y)
    finalbetween = bet.map(lambda x: ((x[0][0],x[0][1]),float(x[1]/2))).sortBy(lambda x: x[0][0]).sortBy(lambda x: x[1], ascending=False)
    to_print =finalbetween.collect()
    between_dict = dict(to_print)
    with open(between_output_path, 'w') as f:
        for item in to_print:
            convert = str(item)
            f.write("%s" % convert[:-1].replace("[", "").replace("]", "").replace("((", "("))
            f.write("\n")
        f.close()
    global finalcomm
    edge_count = int(finalbetween.count())
    finalmod =0
    origgraph_dict = dict(pair_dict)
    #while i < edge_count:
    while between_dict:
        #print("i===",i)
        remove_edges = []
        maxval = list(between_dict.values())[0]
        #print("max",maxval)
        for key,val in between_dict.items():
            if maxval == val:
                remove_edges.append(key)
        for edge in remove_edges:
            elem1= edge[0]
            elem2= edge[1]
            #print(elem1,',',elem2)
            pair_dict[elem1].remove(elem2)
            pair_dict[elem2].remove(elem1)
        between_dict, community_list = newbtw(pair_dict)
        for k,v in between_dict.items():
            between_dict[k] = float(v)/2
        between_dict = OrderedDict(sorted(between_dict.items(), key=lambda x: x[1], reverse=True))
        mod = getModularity(origgraph_dict,community_list,edge_count)
        if mod > finalmod:
            finalmod = mod
            finalcomm = list(community_list)
            #print("commmmmm",len(finalcomm))

    finalcomm = sorted(finalcomm,key= lambda x: x[0])
    printing_comm = sorted(finalcomm,key = lambda x: len(x))
    with open(comm_output_path, 'w') as file:
        for item in printing_comm:
            file.write("%s" % str(item).replace("(", "").replace(")", "").replace("((", "("))
            file.write("\n")
        file.close()

    end= time.time()
    print("Duration ", end-start)