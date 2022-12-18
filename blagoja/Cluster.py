from utils.helper import initalizeGraphSpark,loadFile, gini
from pyspark.sql.functions import *
# from graphframes import *
import pandas as pd
import numpy as np
import glob, os
import networkx as nx
from networkx.algorithms import approximation

# Paths
test_txs_path="/home/ubuntu/NSProject/dataset/dataset-new/raw/all_txs"
destination_path_nodes="/home/ubuntu/NSProject/dataset/dataset-new/stage/Cluster"
destination_path_stats="/home/ubuntu/NSProject/dataset/dataset-new/stage/Stats/Cluster"

# Variables
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["from","to","relationship"]

final_columns_nodes = ["cluster", "time_week"]
final_columns_stats = ["mean_cluster", "std_cluster", "gini_coefficient", "time_week" ]

spark = initalizeGraphSpark("Cluster")
# df_accounts = loadFile(spark, accounts_path, True ).filter(df_accounts['Type'] == 1)

# List out all directories in the destination 
listDesDir =[x[0].split("/")[-1] for x in os.walk(destination_path_nodes)]

# List out all directories in the source
listSrcDir  = [x[0] for x in os.walk(test_txs_path)]

# This will loop over each weekly transactions folder in src and process a graph logic for the analysis.
for x in listSrcDir:
    if x.find("date=2022")  != -1:
        dirname = x.split("/")[-1]
        if not (dirname in listDesDir):

            # Load the each weekly trnsactions file and selecting only relevant columns
            df_new = loadFile(spark, x, True ).select(e1_cols)
            
            # Rename the columns, from_pandas_edgelist() only accept
            df_final = df_new.withColumnRenamed("SenderId","from").withColumnRenamed("TargetId","to")

            # Null checks on the essential columns
            e1 = df_final.filter(col("from").isNotNull()).filter(col("to").isNotNull()) \
                .groupBy("from","to").count().withColumn("relationship",lit('txs')).select(e1_final).toPandas()

            # Create a directional graph from edgelist from Pandas
            G = nx.from_pandas_edgelist(e1, "from", "to", create_using=nx.DiGraph())
            cluster_nodes = approximation.average_clustering(G,trials=1000, seed=10)
            cluster_mean = np.array(list(cluster_nodes.values())).mean()
            cluster_std = np.array(list(cluster_nodes.values())).std()
            cluster_gini = gini(np.array(list(cluster_nodes.values())))


            #cluster_nodes = [(str(cluster_nodes), dirname.split("=")[-1])]
            cluster_stats = [(str(cluster_mean), str(cluster_std), str(cluster_gini), dirname.split("=")[-1])]
            #df_cluster_nodes = spark.createDataFrame(cluster_nodes).toDF(*final_columns_nodes)
            df_cluster_stats = spark.createDataFrame(cluster_stats).toDF(*final_columns_stats)

            #Show the Output in the terminal
            #df_cluster_nodes.show()
            #df_cluster_stats.show()

            #Write the rows in a directory
            #df_cluster_nodes.write.option("header", True).mode('overwrite').csv(destination_path_nodes+"/"+dirname)
            df_cluster_stats.write.option("header", True).mode('overwrite').csv(destination_path_stats+"/"+dirname)

spark.stop()