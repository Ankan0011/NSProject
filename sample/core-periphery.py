import pandas as pd
import cpnet, os
import networkx as nx
import numpy as np
from utils.helper import initalizeGraphSpark,loadFile
from pyspark.sql.functions import *

# Path of the source and destination folder
test_txs_path="/home/ubuntu/NSProject/dataset/dataset-new/raw/all_txs"
destination_path="/home/ubuntu/NSProject/dataset/dataset-new/curated/core-periphery"

# Variables
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["src","dst","relationship"]

spark = initalizeGraphSpark("Core-Periphery")

listSrcDir  = [x[0] for x in os.walk(test_txs_path)]

# List out all directories in the destination 
listDesDir =[x[0].split("/")[-1] for x in os.walk(destination_path)]

alg = cpnet.Lip() #Fast Bipartition Algo

def count_dict(core, weekno):
    core_count = 0
    for i in core.values():
        if i == 1:
            core_count += 1
    return [(weekno, core_count, len(core))]


for x in listSrcDir:
    if x.find("date=20")  != -1:
        dirname = x.split("/")[-1]
        if not (dirname in listDesDir):
            print(dirname)
            weekno=dirname.split("=")[-1]
            df_new = loadFile(spark, x, True ).select(e1_cols)
            
            # Rename the columns, from_pandas_edgelist() only accept
            df_temp = df_new.withColumnRenamed("SenderId","src").withColumnRenamed("TargetId","dst")

            df_final = df_temp.groupBy("src","dst").count().withColumn("relationship",lit('txs')).select(e1_final).toPandas()
            G = nx.from_pandas_edgelist(df_final, "src", "dst")
            alg.detect(G)
            x = alg.get_coreness()  # Get the coreness of nodes
            df_response = pd.DataFrame.from_records(count_dict(x, weekno), columns =['week_no', 'core_count', 'network_size'])
            spark_df = spark.createDataFrame(df_response)
            # spark_df.show()
            spark_df.write.option("header", True).mode('overwrite').csv(destination_path+"/"+dirname)


# x = alg.get_coreness()  # Get the coreness of nodes
# c = alg.get_pair_id()  # Get the group membership of nodes