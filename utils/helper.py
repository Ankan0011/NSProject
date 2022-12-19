from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *
from datetime import datetime
import numpy as np


#Graph Spark packages
def initalizeGraphSpark(AppName):
        print("Importing the Graph Initialization")
        spark = SparkSession.builder.appName(AppName) \
                .config("spark.driver.extraJavaOptions", "-Xss400M") \
                .config("spark.driver.extraClassPath","/home/ubuntu/NSProject/packages/graphframes-0.8.1-spark3.0-s_2.12.jar") \
                .config("spark.executor.memory", "2g") \
                .config("spark.executor.cores", "2") \
                .config('spark.cores.max', '4') \
                .config('spark.driver.memory','2g') \
                .config('spark.executor.instances','3') \
                .config('spark.dynamicAllocation.enabled', 'true') \
                .config('spark.dynamicAllocation.shuffleTracking.enabled', 'true') \
                .config('spark.dynamicAllocation.executorIdleTimeout', '60s')\
                .config('spark.dynamicAllocation.minExecutors', '0')\
                .config('spark.dynamicAllocation.maxExecutors', '5')\
                .config('spark.dynamicAllocation.initialExecutors', '1')\
                .config('spark.dynamicAllocation.executorAllocationRatio', '1')\
                .config('spark.worker.cleanup.enabled', 'true') \
                .config('spark.worker.cleanup.interval', '60') \
                .config('spark.shuffle.service.db.enabled', 'true') \
                .config('spark.worker.cleanup.appDataTtl', '60') \
                .config('spark.excludeOnFailure.killExcludedExecutors','true') \
                .config('spark.sql.debug.maxToStringFields', 100).getOrCreate()
        spark.sparkContext.setCheckpointDir(dirName="/home/ubuntu/NSProject/dataset/dataset-new/temp")
        # spark.sparkContext.setSystemProperty('logger.File', '/mnt/indexer-build/migrated_data/logs/sample.log')
        # logger = spark._jvm.org.apache.log4j.LogManager.getLogger('default')

        return spark

def loadFile(spark, path, header):
        print("Loading the csv files")
        df = spark.read.option("header", header).option("inferSchema", "true") \
                .option("ignoreLeadingWhiteSpace", "true") \
                .option("ignoreTrailingWhiteSpace", "true") \
                .csv(path+"/*.csv")
        return df

def gini(x):
    total = 0
    for i, xi in enumerate(x[:-1], 1):
        total += np.sum(np.abs(xi - x[i:]))
    return total / (len(x)**2 * np.mean(x))
