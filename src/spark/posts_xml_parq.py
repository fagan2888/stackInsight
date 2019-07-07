from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as fI
from pyspark import SparkContext,SparkConf
import re,html
from boto3 import client
import csv

sc = SparkContext(conf=SparkConf().setAppName("se"))
spark = SparkSession.builder.appName("se").getOrCreate()
pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')
parse_line = lambda line: {key:value for key,value in pattern.findall(line)}
unescape = udf(lambda escaped: html.unescape(escaped) if escaped else None)

def read_tags_raw(tags_string): 
# converts tags of the form <tag1><tag2> to ['tag1', 'tag2']
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []
    
read_tags = udf(read_tags_raw, ArrayType(StringType()))

#boto3 function to read extract file names from the s3 bucket"
def file_names_retrieve():
    conn = client('s3')
    posts_file_names = []
    for key in conn.list_objects(Bucket='xmlsefiles')['Contents']:
       file_name = key['Key'].split('/')[0]
       if "posts" in file_name:
          posts_file_names.append(file_name)
    return posts_file_names
  
  
xml_files = file_names_retrieve()
#for xml_file
posts_udf = spark.read.text(xml_files).where(col('value').like('%<row Id%')) \
                 .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
                 .select(col('value.Id').cast('integer'),
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
                 )
file_name_df = spark.read.text(xml_files).where(col('value').like('%<row Id%')).select(input_file_name())
#extract the correct community name here:
#community_name_df = file_name_df.withColumn('Community', regexp_extract(col('input_file_name()'), './(posts)([\w\.]+)-Posts-([\d]+).xml',2))
community_name_df = file_name_df.withColumn('Community', regexp_extract(col('input_file_name()'), './(posts)([\w\.]+)',2))

clean_name_df_subset = community_name_df.select("Community")

posts_udf_id = posts_udf.withColumn("iid", monotonically_increasing_id())
clean_name_df_subset_id = clean_name_df_subset.withColumn("iid", monotonically_increasing_id())


community_posts = posts_udf_id.join(clean_name_df_subset_id, posts_udf_id.iid == clean_name_df_subset_id.iid).drop("iid")
community_posts.write.parquet("s3a://parquetoutputse/posts.parquet")

spark.catalog.clearCache()
