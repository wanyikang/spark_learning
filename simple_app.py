# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from os.path import expanduser, join

logFile = '/home/richard_wan/Projects/spark-2.3.0-bin-hadoop2.7/README.md'
spark = SparkSession.builder.appName('SimpleApp').getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print('Lines with a: %i, lines with b: %i' % (numAs, numBs))

spark.stop()

