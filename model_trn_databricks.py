#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 14 23:39:00 2019

@author: jitengirdhar
"""

#./bin/spark-shell --driver-memory 5g
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import (StructField, StringType, StructType, IntegerType, DoubleType)
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
#import re
#spark.executor.memory=2g



#print(userdat_rdd.take(1))
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("hwk2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    
   
    sqlContext = SQLContext(sc)

    data_schema = [StructField('label', IntegerType(), True), StructField('text', StringType(), True)]
    final_struc = StructType(fields = data_schema)
#    rd1 = sc.textFile("trndata.csv").map(lambda x: x.split("|")).map(lambda x:[int(x[0]),x[1]])
#    df1 = spark.createDataFrame(rd1, final_struc)
#    print(df1.take(1))
    
    df1 = sqlContext.read.format('csv').options(header='true',schema=final_struc,delimiter='|').load('/FileStore/tables/trndata.csv')
    df1 = df1.withColumn("label", df1["label"].cast(IntegerType()))
    df1 = df1.withColumn("text", df1["text"].cast(StringType()))
    #print(df1.take(1))
##    
    trn_data, tst_data = df1.randomSplit([0.6, 0.4], seed=0)    #training and test split 
    
    tokenizer = Tokenizer(inputCol="text", outputCol="words")    
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")  
    #added spark modifiers as specified in the question
    #print(df1.take(1))
    
    lr = LogisticRegression(maxIter=2, regParam=0.001)
    # logistic regression with iteartion =2 due to space constraint on databricks as well as my local machine
    # JVM on my system keeps running out of space
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
    # pipeline the model stage
    
    model = pipeline.fit(trn_data)
    # Fit the pipeline to training data.

    m = model.transform(tst_data).select("features", "label", "prediction")
    # model saved
  
    #print(m.take(5))
    
    crrct_classifications = m.where(m["label"] == m["prediction"])
    # check for correct classifications
    accuracy = crrct_classifications.count()/test.count()
    print("Accuracy of model ="+ format(accuracy*100))
    model.save("/FileStore/hwk3_lr_modelf")
    #model save for deployment to kafka