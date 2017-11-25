from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import socket
import threading
import time
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
    #A stands for retrieving the first three lines of data from the airline table
    if line=='A':
        res=reader('D:\\MSBD5003\\flights.csv')
        return str(res)






def tcplink(sock,addr):

    print('handling new connection')
    sock.send(b'connected to pyspark server')
    while True:
        data = sock.recv(1024)
        data1=bytes.decode(data)
        result=parse_and_do(data1)
        time.sleep(1)
        if data1 == 'exit' or not data1:
            break
        send_data='Hello '+data1
        sock.send(str.encode(send_data))
    #sock.close()








if __name__=='__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 9999))
    s.listen(5)
    print('waiting for connections')
    while True:

      sock, addr = s.accept()

      t = threading.Thread(target=tcplink, args=(sock, addr))
      t.start()
