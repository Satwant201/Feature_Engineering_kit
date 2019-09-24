from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example ::::::").enableHiveSupport() \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

from feature_creation import *

df = spark.read.orc("//hdfs location for the analytical data set using which features are to be created ")


parent_entropy = entropy_lookback(df,"<parent column>","<child_column>","lookback_date",<lookback prediod>,<training_period_n_months>)


#### event column should be a binary column 0/1 only 
parent_risk = risk_standard(df,"<parent column>","<event_column>","lookback_date",<lookback prediod>,<training_period_n_months>)

parent_risk_adjusted = risk_adjusted(df,<parent column>","<event_column>","dollar_column","lookback_date")

column_lb_billed_amt = look_back_fxn_test(df,["levelA","levelB",....."levelN"],"lookback_date","dollar_column","mean",6,n_months)



