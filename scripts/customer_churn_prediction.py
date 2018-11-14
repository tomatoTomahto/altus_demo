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
      .appName("Predict Customer Churn") \
      .getOrCreate()
    
## ## Configure Data Schema
#customerSchema = StructType([StructField("id", IntegerType(), True),
#                             StructField("state", StringType(), True),
#                             StructField("area_code", StringType(), True),
#                             StructField("phone_number", StringType(), True),
#                             StructField("intl_plan", BooleanType(), True),
#                             StructField("voice_mail_plan", BooleanType(), True),
#                             StructField("number_vmail_messages", DoubleType(), True),     
#                             StructField("total_day_minutes", DoubleType(), True),     
#                             StructField("total_day_calls", DoubleType(), True),     
#                             StructField("total_day_charge", DoubleType(), True),     
#                             StructField("total_eve_minutes", DoubleType(), True),     
#                             StructField("total_eve_calls", DoubleType(), True),     
#                             StructField("total_eve_charge", DoubleType(), True),     
#                             StructField("total_night_minutes", DoubleType(), True),     
#                             StructField("total_night_calls", DoubleType(), True),     
#                             StructField("total_night_charge", DoubleType(), True),     
#                             StructField("total_intl_minutes", DoubleType(), True),     
#                             StructField("total_intl_calls", DoubleType(), True),     
#                             StructField("total_intl_charge", DoubleType(), True),     
#                             StructField("number_customer_service_calls", DoubleType(), True),
#                             StructField("account_length", DoubleType(), True),
#                             StructField("churned", BooleanType(), True)])
#
#churnData = spark.read.schema(customerSchema).csv(os.path.join(s3DataDir,'customers'))

# # Data Loading
# ## Read in Churn Data from S3
churnData = spark.read.parquet(os.path.join(s3DataDir,'customers'))

customers.persist()
customers.show(5)


reduced_numeric_cols = ["account_length", "number_vmail_messages", "total_day_calls",
                        "total_day_charge", "total_eve_calls", "total_eve_charge",
                        "total_night_calls", "total_night_charge", "total_intl_calls", 
                        "total_intl_charge","number_customer_service_calls"]


from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
label_indexer = StringIndexer(inputCol = 'churned', outputCol = 'label')
plan_indexer = StringIndexer(inputCol = 'intl_plan', outputCol = 'intl_plan_indexed')
input_cols=['intl_plan_indexed'] + reduced_numeric_cols
assembler = VectorAssembler(
    inputCols = input_cols,
    outputCol = 'features')

param_numTrees=10
param_maxDepth=5
param_impurity='gini'

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(labelCol = 'label', 
                                    featuresCol = 'features', 
                                    numTrees = param_numTrees, 
                                    maxDepth = param_maxDepth,  
                                    impurity = param_impurity)
pipeline = Pipeline(stages=[plan_indexer, label_indexer, assembler, classifier])
(train, test) = churn_data.randomSplit([0.7, 0.3])
model = pipeline.fit(train)

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import udf
predictions = model.transform(test)
evaluator = BinaryClassificationEvaluator()
auroc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})
aupr = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderPR"})
"The AUROC is %s and the AUPR is %s" % (auroc, aupr)

