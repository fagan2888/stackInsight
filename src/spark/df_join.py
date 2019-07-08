from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import re,html
from pyspark.sql.functions import broadcast
#set up a spark session:

#read in all the links parquet files:
#set up a spark session:
sc = SparkContext(conf=SparkConf().setAppName("se"))


spark = SparkSession.builder.appName("se").getOrCreate()
#spark.sql("set spark.sql.caseSensitive=true")
links = spark.read.load("s3a://parquetoutputse/pr_se_links.parquet")
print(links.columns)


import boto3
from boto3 import client
import csv
conn = client('s3')

s3 = boto3.resource('s3')


bucket = s3.Bucket('parquetoutputse')
folders = []
def file_names_retrieve():
    conn = client('s3')
    posts_file_names = []
    for key in conn.list_objects(Bucket='parquetoutputse')['Contents']:
       file_name = key['Key'].split('/')[0]
       if "posts" in file_name:
          posts_file_names.append(file_name)
    return posts_file_names

posts_parquet_files = file_names_retrieve()
parquet_files = ["s3a://parquetoutputse/" + posts_parquet_file for posts_parquet_file in posts_parquet_files]

df3 = spark.read.load(parquet_files)

questions = df3.filter((f.col('PostTypeId')==1)).filter((f.col('AcceptedAnswerId').isNotNull()))
questions_subset = questions.select('Id','AcceptedAnswerId','Tags','CreationDate', 'Community')
answers = df3.filter((f.col('PostTypeId')==2))
answers_subset = answers.select("Id","CreationDate")
new_names = ['AnsId', 'AnsCreationDate']
answers_subset = answers_subset.toDF(*new_names)

qa_deets = questions_subset.join(answers_subset,questions_subset.AcceptedAnswerId == answers_subset.AnsId)
timeFmt = "yyyy-MM-dd' 'HH:mm:ss.SSS"
timeDiff = (f.unix_timestamp('AnsCreationDate', format=timeFmt)
            - f.unix_timestamp('CreationDate', format=timeFmt))

qa_deets = qa_deets.withColumn("Duration", timeDiff/60)
qa_deets_subset = qa_deets.select("Id",  "Tags","CreationDate","Community", "Duration")

questions_null = df3.filter((f.col('PostTypeId')==1)).filter((f.col('AcceptedAnswerId').isNull()))
questions_null_subset = questions_null.select('Id','Tags','CreationDate','Community')
questions_null_duration = questions_null_subset.withColumn('Duration', lit(None).cast(DoubleType()))


all_questions = qa_deets_subset.union(questions_null_duration)




all_questions = all_questions.withColumn('post_create_date',all_questions['CreationDate'].cast('date'))

all_questions = all_questions.withColumnRenamed('id','COMMUNITY_QUESTION_ID')
all_questions_subset = all_questions.select('COMMUNITY_QUESTION_ID','Community','Tags', 'post_create_date')

#cred_tags = links.join(all_questions, (links.id == all_questions.COMMUNITY_QUESTION_ID) & (all_questions_subset.Community == links.community)
#)
links  = links.withColumnRenamed("community","lcommunity")
cred_tags = all_questions.join(broadcast(links), (links.id == all_questions.COMMUNITY_QUESTION_ID) & (all_questions_subset.Community == links.lcommunity), "left_outer")



total_df = cred_tags.select("COMMUNITY_QUESTION_ID","Community","post_create_date","Tags","Duration","cred_score")


total_df  = total_df.withColumn("duration",f.round(total_df["Duration"],2))
total_df  = total_df.withColumn("pr_score",f.round(total_df["cred_score"],3))
total_df  = total_df.withColumnRenamed("COMMUNITY_QUESTION_ID","qid")
total_df  = total_df.withColumnRenamed("Community","community")
total_df  =  total_df.withColumnRenamed("Tags","tags")
total_df  = total_df.withColumnRenamed("post_create_date","create_date")
total_df_reqd = total_df.select("qid","tags","community","duration","create_date","pr_score")
total_df_reqd.write.format("jdbc").mode("append") .option("url", "jdbc:postgresql://hostname/ls?user=postgres&password=").option("dbtable", "questions").option("user", "postgres").option("password", "").save()
spark.catalog.clearCache()
