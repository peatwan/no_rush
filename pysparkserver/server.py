from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import socket
import threading
import time
import json

sc=SparkContext('local')

spark = SparkSession \
    .builder \
    .appName('name') \
    .config("spark.sql.shuffle.partitions",5) \
    .getOrCreate()


def reader(dir):
    df=spark.read.csv(dir)

    col=list(df.toPandas.iloc[:3,0])
    return col



def list2str(ls):
  pass





def parse_and_do(line):
    #get stands for retrieving the first three lines of data from the airline table
    string=json.loads(line)

    method=string['method']
    time=string['time']
    h=time.split(':')[0]
    min=time.split(':')[1]
    origin=string['origin']
    back=prediction(origin+','+h+','+min)

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
