#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 14 22:33:20 2019

@author: jitengirdhar
"""


from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from kafka import KafkaConsumer


sc = SparkContext.getOrCreate();

sqlContext = SQLContext(sc)

#topic = "guardian_data"

consumer = KafkaConsumer('guardian_data', bootstrap_servers = ['localhost:9092'])

model_dir = r"./hwk3_bgm"              
model = PipelineModel.load(model_dir)

for text in consumer:
    data = str(text.value, 'utf-8')
    inp = data.split("|")
    index = inp[0]
    message = inp[1]
    message = '"'+message.replace("\n","").replace("<p>"," ").replace("</p>"," ").replace("<br>"," ")+'"'
    
    incoming = sc.parallelize([{'label':index, 'text':message}]).toDF()
    #liveData = sc.parallelize([{'category': index, 'news': body}]).toDF()  
        #liveData.show()
    result = model.transform(incoming).select("label","prediction")
    result.show()
    print(result)
    
    
    
