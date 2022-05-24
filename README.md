#we imported Pandas, SPark.sql, Numpy,Pyplot and sealine

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline
import pyspark.sql.functions as F

#spark session imported

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Project1").master("local").getOrCreate()

spark

#bank data uplaoded from the FTP Server

bank_data = spark.read.format("csv").option('delimiter', '\t').load('Project 1_dataset_bank-full (2).csv')

bank_data.show()

bank_data.printSchema()

#printing schema of bank data

bank_data.show(2, truncate = False)

## We need to remove the double quotes in the values to extract the dataframe in the right format

bank_data1 = bank_data.withColumn('_c0', regexp_replace('_c0', '"', '')).rdd

bank_data1.take(10)

bank_data2 = bank_data1.map(lambda x: x[0].split(";"))
header = bank_data2.first()

bd_data3 = bank_data2.filter(lambda x: x!= header)
bd_df =bd_data3.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6], x[7], x[8],\
                                   x[9], x[10], int(x[11]), int(x[12]), int(x[13]), int(x[14]), x[15], x[16]))

df = bd_df.toDF(header)

df.show(10)

df.columns

# Give marketing success rate (No. of people subscribed / total no. of entries 

# saving the df to view to do analysis on top of it
df.createOrReplaceTempView('df')

spark.sql(""" select age from df where age== 0 """).show()
## after explicit conversion there is no value in age column that corresponds to missing/ null value

## Total no of enteries are 45211
spark.sql("""select count(*) from df """).show()

spark.sql("""select  round((count(y)/45211) * 100, 2) as market_success_rate from df where y = 'yes' """).show()

# Give Market Failure Rate

##Give the maximum, mean, and minimum age of the average targeted customer

spark.sql (""" select max(age), min(age), round(mean(age), 2) from df where y == 'no' """).show()

##Check the quality of customers by checking average balance, median balance of customers

ordered_df = spark.sql("""select * from df where y == 'no' order by balance """)
ordered_df.createOrReplaceTempView("ordered_df")

spark.sql("""select AVG(balance) as Mean_Amt, PERCENTILE(balance, 0.5) as Median_Amt FROM ordered_df """).show()

##Check if age matters in marketing subscription for deposit

## We have created age bins to help visualize how different segments responded

spark.sql("""select age, count(y) as number from df where y == 'no' group by age order by number desc """).\
withColumn("agebins", F.when(df.age < 30, '<30')\
           .when((df.age>= 30) & (df.age < 35), '30-35')\
           .when((df.age>= 35) & (df.age < 40), '35-40')\
           .when((df.age>= 40) & (df.age < 45), '40-45')\
           .when((df.age>= 45) & (df.age < 50), '45-50')\
           .when((df.age>= 50) & (df.age < 60), '50-60')\
           .otherwise('+60')).createOrReplaceTempView("agebins")
                

plot_df= spark.sql("select agebins, sum(number) as no_response from agebins group by agebins ").toPandas()

plot_df

sns.barplot(data = plot_df, x = 'agebins', y = 'no_response', color = 'khaki')
plt.title("No subscription across different Age Bins")
plt.show()

##Check if marital status mattered for a subscription to deposit

spark.sql("""select marital, count(y) as number from df where y LIKE 'no' group by marital order by number desc """).show()

