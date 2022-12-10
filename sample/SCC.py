from utils.helper import initalizeGraphSpark,loadFile
from pyspark.sql.functions import *
# from graphframes import *
import pandas as pd
import glob, os
import networkx as nx

# Paths
test_txs_path="/home/ubuntu/NSProject/dataset/dataset-new/raw/all_txs"
destination_path="/home/ubuntu/NSProject/dataset/dataset-new/stage/SSC"

# Variables
e1_cols=["SenderId","TargetId","Year_no","Week_no"]
e1_final=["from","to","relationship"]
final_columns = ["degree_assort","time_week"]

spark = initalizeGraphSpark("Degree Assortativity")
# df_accounts = loadFile(spark, accounts_path, True ).filter(df_accounts['Type'] == 1)

# List out all directories in the destination 
listDesDir =[x[0].split("/")[-1] for x in os.walk(destination_path)]

# List out all directories in the source
listSrcDir  = [x[0] for x in os.walk(test_txs_path)]

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
            try:
                # Create a directional graph from edgelist from Pandas
                G = nx.from_pandas_edgelist(e1, "from", "to", create_using=nx.DiGraph())
                coeff = nx.degree_assortativity_coefficient(G)
                # print("Coefficient is :"+str(coeff))

                data = [(str(coeff), dirname.split("=")[-1])]
                next = spark.createDataFrame(data).toDF(*final_columns)
                next.show()

                # Uncomment the below line to write the rows in a directory
                # next.write.option("header", True).mode('overwrite').csv(destination_path+"/"+dirname)
            except NameError:
                print("Exception")
                print(NameError)

spark.stop()