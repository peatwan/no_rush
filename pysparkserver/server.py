# from pyspark import SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import socket
import threading
import time
import json
import numpy as np
from collections import OrderedDict
import test_prediction
# sc=SparkContext('local')

# spark = SparkSession \
#     .builder \
#     .appName('name') \
#     .config("spark.sql.shuffle.partitions",5) \
#     .getOrCreate()


# def reader(dir):
#     df=spark.read.csv(dir)

#     col=list(df.toPandas.iloc[:3,0])
#     return col



def list2str(ls):
  pass


def dict2list(dic:dict):

    keys = dic.keys()
    vals = dic.values()
    lst = [(key, val) for key, val in zip(keys, vals)]
    return lst




def dict_rank(d):
   return sorted(dict2list(d), key=lambda x: x[1], reverse=False)



def tuple2json(tp):
    dic=OrderedDict()
    for t in tp:
        dic[t[0]]=t[1]
    return json.dumps(dic)










def parse_and_do(line):
    #get stands for retrieving the first three lines of data from the airline table
    string=json.loads(line)

    method=string['method']
    time=string['time']
    h=time.split(':')[0]
    min=time.split(':')[1]
    origin=string['origin']
    back=test_prediction.prediction(origin+','+h+','+min)
    back=dict_rank(back)
    back=tuple2json(back)

    if method=='get':
          return back





def tcplink(sock,addr):

    print('handling new connection')

    while True:
        data = sock.recv(1024)
        data1=bytes.decode(data)
        result=parse_and_do(data1)
        result=json.dumps(result)
        time.sleep(1)
        if data1 == 'done' or not data1:
            break

        sock.send(str.encode(result))
    sock.close()








if __name__=='__main__':




    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('10.0.0.5', 9999))
    s.listen(5)
    print('waiting for connections')
    while True:

      sock, addr = s.accept()

      t = threading.Thread(target=tcplink, args=(sock, addr))
      t.start()


