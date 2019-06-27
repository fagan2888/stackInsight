from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import re,html,csv
from itertools import chain, combinations
from datetime import datetime

def format_timestamp(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

# Create the UDF
format_timestamp_udf = udf(lambda x: format_timestamp(x))

#set up a spark session:
sc = SparkContext(conf=SparkConf().setAppName("se"))

#spark = SparkSession.builder.master("local[*]").getOrCreate()
spark = SparkSession.builder.appName("se").getOrCreate()

#find the parquet files which contain the files:
#askubuntu,mathse,physics,suser
parquet_files = ["tmb0.parquet","tmb10.parquet", "t2mb100.parquet","t2mb110.parquet","t2mb140.parquet","t2mb150.parquet","t2mb170.parquet","t2mb180.parquet"]

#load the files
print("STAGE 1: LOAD THE FILES")
df3 = spark.read.load("s3a://parquetoutputse/posts_se_community_tmb0.parquet")
questions = df3.filter((f.col('PostTypeId')==1)).filter((f.col('AcceptedAnswerId').isNotNull()))
answers = df3.filter((f.col('PostTypeId')==2))

print("STAGE 2:PERFORM TRANSFORMATION FOR DURATION")
#question-answer join to answer the duration 
questions_subset = questions.select('Id','AcceptedAnswerId','Tags','CreationDate','Community')

answers_subset = answers.select("Id","CreationDate")
new_names = ['AnsId', 'AnsCreationDate']
answers_subset = answers_subset.toDF(*new_names)

qa_deets = questions_subset.join(answers_subset,questions_subset.AcceptedAnswerId == answers_subset.AnsId)

timeFmt = "yyyy-MM-dd' 'HH:mm:ss.SSS"
timeDiff = (f.unix_timestamp('AnsCreationDate', format=timeFmt)
            - f.unix_timestamp('CreationDate', format=timeFmt))

qa_deets = qa_deets.withColumn("Duration", timeDiff/60)
qa_deets_subset = qa_deets.select("Id",  "Tags","CreationDate","Community", "Duration")

#append the questions which has answers ; hence no duration to question_deets_subset:
questions_null = df3.filter((f.col('PostTypeId')==1)).filter((f.col('AcceptedAnswerId').isNull()))
questions_null_subset = questions_null.select('Id','Tags','CreationDate','Community')
questions_null_duration = questions_null_subset.withColumn('Duration', lit(None).cast(DoubleType()))

#rename question id to community question id:
all_questions = qa_deets_subset.union(questions_null_duration)
print("STAGE 3:GENERATE SUBSETS")

allsubsets = lambda l: [[z for z in y] for y in chain(*[combinations(l , n) for n in range(1,len(l)+1)])]


df = all_questions.withColumn('tagSubsets',udf(allsubsets)(all_questions['tags']))
df = df.withColumn('tagSubsets', udf(allsubsets, ArrayType(ArrayType(StringType())))(df['tagSubsets']))
#df = df.withColumn('tagSubsetsc',udf(tagsubssets,ArrayType(StringType()))(df['tagSubsets'])
print("STAGE 4:EXPLODE COLUMNS AND REVISE THE DATE")
#explode the columns for the subsets:
df = df.withColumn('tagExplode', explode('tagSubsets'))

df.select('tagExplode','CreationDate','Duration').write.format("jdbc").mode("append") .option("url", "jdbc:postgresql://ec2-18-205-56-13.compute-1.amazonaws.com/ins_se?user=postgres&password=*").option("dbtable", "rt_table").option("user", "postgres").option("password", "*").save()
