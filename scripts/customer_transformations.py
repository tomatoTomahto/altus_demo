# # Initialization
# ## Import Python and PySpark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd
import sys, os

# ## Configure Data Directory
s3DataDir = 's3a://sgupta-s3/DStelco/data'
if len(sys.argv)>1:
  s3DataDir = sys.argv[1]

# ## Connect to Spark
spark = SparkSession.builder \
      .appName("Transform Customer Data") \
      .getOrCreate()

# ## Prepare Data Schemas
custInfoSchema = StructType([StructField("id", IntegerType(), True),
                             StructField("state", StringType(), True),
                             StructField("area_code", StringType(), True),
                             StructField("phone_number", StringType(), True),
                             StructField("intl_plan", StringType(), True),
                             StructField("voice_mail_plan", StringType(), True)])
custMetricSchema = StructType([StructField("id", IntegerType(), True),
                               StructField("number_vmail_messages", DoubleType(), True),     
                               StructField("total_day_minutes", DoubleType(), True),     
                               StructField("total_day_calls", DoubleType(), True),     
                               StructField("total_day_charge", DoubleType(), True),     
                               StructField("total_eve_minutes", DoubleType(), True),     
                               StructField("total_eve_calls", DoubleType(), True),     
                               StructField("total_eve_charge", DoubleType(), True),     
                               StructField("total_night_minutes", DoubleType(), True),     
                               StructField("total_night_calls", DoubleType(), True),     
                               StructField("total_night_charge", DoubleType(), True),     
                               StructField("total_intl_minutes", DoubleType(), True),     
                               StructField("total_intl_calls", DoubleType(), True),     
                               StructField("total_intl_charge", DoubleType(), True),     
                               StructField("number_customer_service_calls", DoubleType(), True)])
custChurnSchema = StructType([StructField("id", IntegerType(), True),
                              StructField("account_length", DoubleType(), True),
                              StructField("churned", StringType(), True)])

# # Data Loading
# ## Read Data From S3
custInfo = spark.read.schema(custInfoSchema).csv(os.path.join(s3DataDir,'customer_info.csv'))
custMetrics = spark.read.schema(custMetricSchema).csv(os.path.join(s3DataDir,'customer_metrics.csv'))
custChurn = spark.read.schema(custChurnSchema).csv(os.path.join(s3DataDir,'customer_churn.csv'))

# # Data Transformations
# ## Join Datasets Into a Wide Table
customers = custInfo.join(custMetrics, 'id')\
  .join(custChurn, 'id')
  
customers.persist()
customers.show(5)

# ## Convert String Columns to Boolean
customers = customers.withColumn('intl_plan',F.trim(customers.intl_plan)=='yes')\
  .withColumn('voice_mail_plan',F.trim(customers.voice_mail_plan)=='yes')\
  .withColumn('churned',F.trim(customers.churned)=='True.')

customers.printSchema()

# ## Write Transformed Data back to S3
customers.coalesce(1).write.parquet(path=os.path.join(s3DataDir,'customers'), mode='overwrite')