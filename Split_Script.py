
import pyspark
import os
import pandas
import distutils
import csv
import sqlite3
from sqlalchemy import create_engine
import datetime


from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("JEH").getOrCreate()

maindf=spark.read.option("header",True).option("escape",'"').option("multiLine",True).csv("D:/Data_Engg/JEH/raw.csv")


# Step 1: cols to be splitted
array_cols = []
for c in maindf.columns:

    sorted_df=maindf.orderBy(col(c).asc())

    filtered_df=sorted_df.filter(col(c).startswith("[") & col(c).endswith("]"))

    if filtered_df.count()>0:
        array_cols.append(c)
print(array_cols)
print(len(array_cols))

tmpdf=maindf

final_columns = []
for c in tmpdf.columns:
    
    if c not in array_cols:
        final_columns.append(col(c))
    else:
        # Step 2: Appending original col
        final_columns.append(col(c)) 
        # Step 3: creatimng tmp col to use for conversion of str to arr & splitting data
        tmpcol_name=f"{c}_tmp"
        
        tmpdf = tmpdf.withColumn(tmpcol_name,from_json(col(c), ArrayType(StringType())))

        max_len = tmpdf.select(size(col(tmpcol_name)).alias("len")).agg(F.max("len")).collect()[0][0]

        for i in range(max_len):
            final_columns.append(col(tmpcol_name).getItem(i).alias(f"{c}_{i+1}"))
        
# Step 4: selecting final cols
finaldf = tmpdf.select(*final_columns)

pandas_df = finaldf.toPandas()

# Step 5: saving as CSV
time=datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
pandas_df.to_csv(f"D:/Data_Engg/JEH/output/resultdf_{time}.csv", index=False, sep=',')

# Step 6:  save as table into pg DB
engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")

pandas_df.to_sql(f"splitted_data_{time}", engine, index=False, if_exists="replace") 

