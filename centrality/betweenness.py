from utils.helper import initalizeGraphSpark,loadFile, gini
from pyspark.sql.functions import *
# from graphframes import *
import pandas as pd
import numpy as np
import glob, os
import networkx as nx

# Paths
test_txs_path="/home/ubuntu/NSProject/dataset/dataset-new/raw/all_txs"
destination_path_nodes="/home/ubuntu/NSProject/dataset/dataset-new/stage/Betweenness"
destination_path_stats="/home/ubuntu/NSProject/dataset/dataset-new/stage/Stats/Betweenness"

# Variables
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["from","to","relationship"]
final_columns_nodes = ["betweenness_centrality ", "time_week"]
final_columns_stats = ["mean_betweenness", "std_betweenness", "gini_coefficient", "time_week"]

spark = initalizeGraphSpark("Betweenness Centrality")
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
            betw_nodes = nx.betweenness_centrality(G)
            betw_mean = np.array(list(betw_nodes.values())).mean()
            betw_std = np.array(list(betw_nodes.values())).std()
            betw_gini = cent_gini = gini(np.array(list(betw_nodes.values())))

            #betw_nodes = [(str(betw_nodes), dirname.split("=")[-1])]
            betw_stats = [(str(betw_mean), str(betw_std), str(betw_gini), dirname.split("=")[-1])]
            #df_betw_nodes = spark.createDataFrame(betw_nodes).toDF(*final_columns_nodes)
            df_betw_stats = spark.createDataFrame(betw_stats).toDF(*final_columns_stats)

            #Show the Output in the terminal
            #df_betw_nodes.show()
            #df_betw_stats.show()


            #Write the rows in a directory
            #df_betw_nodes.write.option("header", True).mode('overwrite').csv(destination_path_nodes+"/"+dirname)
            df_betw_stats.write.option("header", True).mode('overwrite').csv(destination_path_stats+"/"+dirname)

spark.stop()