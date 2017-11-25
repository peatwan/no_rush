
# coding: utf-8

import findspark
findspark.init()

from pyspark.ml.regression import LinearRegression,LinearRegressionModel
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.ml import Pipeline,PipelineModel
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.mllib.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.param import Param, Params
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# from IPython.display import display
# from ipywidgets import interact
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from operator import add
import sys
import numpy as np
import pandas as pd
import time
import datetime
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pickle


models={}
all_airlines=['AA','AS',"B6","F9","DL",'EV','HA','MQ','NK','OO',"UA","US","VX","WN"]
sc = SparkContext("local",'app')
spark = SparkSession.builder.appName('name').config('spark.sql.shuffle.partitions',10).getOrCreate()


def init_model():
    all_airlines = ['AA', 'AS','B6',"F9","DL", 'EV', 'HA', 'MQ', 'NK', 'OO', "UA", "US", "VX", "WN"]
    for airline in all_airlines:
        pipeline = PipelineModel.load('model/'+str(airline) + "_pipeline")
        models[str(airline) + "_pipeline"] = pipeline
        model = LinearRegressionModel.load('model/'+str(airline) + "_model")
        models[str(airline) + "_model"] = model
    airport_information=pickle.load(open("model/airport_information.pkl",'rb'))
    models["airport"]=airport_information
    print("load finish")

def prediction(input):
    input=input.split(",")
    hours = (int)(input[1])
    minutes = (int)(input[2])
    times = hours * 60 + minutes
    airport = input[0]
    result={}
    for airline in all_airlines:
        schema = StructType([
            StructField("ORIGIN_AIRPORT", StringType(), nullable=False),
            StructField("AIRLINE", StringType(), nullable=True),
            StructField("DEPARTURE_DELAY", DoubleType(), nullable=True),
            StructField("NEW_SCHEDULED_DEPARTURE", IntegerType(), nullable=True)])
        data = []
        for minute in range(-5, 5, 1):
            data.append((airport, airline, 0.0, times + minute))
        df = spark.createDataFrame(data, schema)
        pipeline = models[str(airline + "_pipeline")]
        if airport not in models["airport"][airline]:
            continue
        data_transformed = pipeline.transform(df)

        data_transformed = data_transformed.withColumnRenamed('Features', 'features')
        selectedcols = ['DEPARTURE_DELAY', "features"]
        dataset_transformed = data_transformed.select(selectedcols)
        dataset_transformed = dataset_transformed.select('*').withColumnRenamed('DEPARTURE_DELAY', 'label')

        model=models[str(airline+"_model")]
        temp_result = model.transform(dataset_transformed).select('prediction').rdd.map(
                lambda element: element['prediction']).collect()
        result[airline]=np.array(temp_result).mean()
        print(airline,result[airline])

    return result


init_model()

# if __name__ == "__main__":
def ma():


    # sc = SparkContext("local",'app')
    # spark = SparkSession.builder.appName('name').config('spark.sql.shuffle.partitions',10).getOrCreate()

    # all_airlines=['AA','AS',"B6","F9","DL",'EV','HA','MQ','NK','OO',"UA","US","VX","WN"]

    # init_model()
    input="BNA,4,20"  #origin_airport+hours+minutes
    result=prediction(input)
    #lines=[origin_airport, airline, schedule_departure_hout,schedule_minute]
    # print(sorted(result.items(),lambda x, y: cmp(x[1], y[1])))


