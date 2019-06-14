from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f

import re,html


spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.read.text('Posts.xml').where(col('value').like('%<row Id%')) \
    .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
    .select(
        col('value.Id').cast('integer'),
        col('value.ParentId').cast('integer'),
        col('value.PostTypeId').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.Score').cast('integer'),
        col('value.ViewCount').cast('integer'),
        unescape('value.Body').alias('Body'),
        col('value.OwnerUserId').cast('integer'),
        col('value.LastActivityDate').cast('timestamp'),
        unescape('value.Title').alias('Title'),
        read_tags('value.Tags').alias('Tags'),
        col('value.CommentCount').cast('integer'),
        col('value.AnswerCount').cast('integer'),
        col('value.LastEditDate').cast('timestamp'),
        col('value.LastEditorUserId').cast('integer'),
        col('value.AcceptedAnswerId').cast('integer'),
        col('value.FavoriteCount').cast('integer'),
        col('value.OwnerDisplayName'),
        col('value.ClosedDate').cast('timestamp'),
        col('value.LastEditorDisplayName'),
        col('value.CommunityOwnedDate').cast('timestamp')    
        ).write.parquet('parqPosts')

pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')
parse_line = lambda line: {key:value for key,value in pattern.findall(line)}
unescape = udf(lambda escaped: html.unescape(escaped) if escaped else None)

def read_tags_raw(tags_string): # converts <tag1><tag2> to ['tag1', 'tag2']
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []
    
read_tags = udf(read_tags_raw, ArrayType(StringType()))


df3 = spark.read.load("parqPosts")



questions = df3.filter((f.col('PostTypeId')==1)).filter((f.col('AcceptedAnswerId').isNotNull()))
questions_subset = questions.select('Id','AcceptedAnswerId','Tags','CreationDate')

answers = df3.filter((f.col('PostTypeId')==2))

answers_subset = answers.select("Id","CreationDate")
new_names = ['AnsId', 'AnsCreationDate']
answers_subset = answers_subset.toDF(*new_names)

qa_deets = questions_subset.join(answers_subset,questions_subset.AcceptedAnswerId == answers_subset.AnsId)
timeFmt = "yyyy-MM-dd' 'HH:mm:ss.SSS"
timeDiff = (f.unix_timestamp('AnsCreationDate', format=timeFmt)
            - f.unix_timestamp('CreationDate', format=timeFmt))







