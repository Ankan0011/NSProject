from utils.helper import initalizeGraphSpark,loadFile
from pyspark.sql.functions import *
# from graphframes import *
import pandas as pd
import numpy as np
import glob, os
import networkx as nx

# Paths
test_txs_path="/home/ubuntu/NSProject/dataset/dataset-new/raw/all_txs"
destination_path_nodes="/home/ubuntu/NSProject/dataset/dataset-new/stage/Degrees1"
destination_path_stats="/home/ubuntu/NSProject/dataset/dataset-new/stage/Stats/Degrees1"

# Variables
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["from","to","relationship"]

final_columns_degree = ["degrees (tuples)", "degrees", "time_week"]
final_columns_stats = ["nr_nodes", "nr_edges", "mean_degree","median_degree", "density", "time_week" ]

spark = initalizeGraphSpark("Degree Distribution")
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
            #Number of Nodes & Edges
            nr_nodes = G.number_of_nodes()
            nr_edges = G.number_of_edges()
            #degree & avg. Degree
            g_deg = nx.degree(G)
            g_degrees = [g_deg[i] for i in G.nodes()]
            median_degree= np.median([tup[1] for tup in g_deg])
            avg_degree = np.mean([tup[1] for tup in g_deg])

            #Density
            density = nx.density(G)

            deg_nodes = [(str(g_deg), str(g_degrees), dirname.split("=")[-1])]
            deg_stats = [(str(nr_nodes), str(nr_edges), str(avg_degree), str(median_degree), str(density), dirname.split("=")[-1])]
            df_degree = spark.createDataFrame(deg_nodes).toDF(*final_columns_degree)
            df_degree_stats = spark.createDataFrame(deg_stats).toDF(*final_columns_stats)

            #Show the Output in the terminal
            df_degree.show()
            df_degree_stats.show()

            #Write the rows in a directory
            #df_degree.write.option("header", True).mode('overwrite').csv(destination_path_nodes+"/"+dirname)
            #df_degree_stats.write.option("header", True).mode('overwrite').csv(destination_path_stats+"/"+dirname)

spark.stop()