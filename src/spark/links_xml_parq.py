from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import re,html,csv
from boto3 import client
import csv


def file_names_retrieve():
    conn = client('s3')
    links_file_names = []
    for key in conn.list_objects(Bucket='xmlsefiles')['Contents']:
       file_name = key['Key'].split('/')[0]
       if "links" in file_name:
          links_file_names.append(file_name)
    return links_file_names
#set up a spark session:
if __name__ == "__main__":
  sc = SparkContext(conf=SparkConf().setAppName("se"))
  spark = SparkSession.builder.appName("se").getOrCreate()
  #preprocessing functions:
  pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')
  parse_line = lambda line: {key:value for key,value in pattern.findall(line)}
  unescape = udf(lambda escaped: html.unescape(escaped) if escaped else None)
  link_file_names = file_names_retrieve()
  link_files = ["s3a://xmlsefiles/" + link_file_name for link_file_name in link_file_names]
  tdf = spark.read.text(link_files).where(col('value').like('%<row Id%')) \
    .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
    .select(
        col('value.Id').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.PostId').cast('integer'),
        col('value.RelatedPostId').cast('integer'),
        col('value.LinkTypeId').cast('integer'),
    )
  fdf = spark.read.text(link_files).where(col('value').like('%<row Id%')).select(input_file_name())
  clean_name_df = fdf.withColumn('Community', regexp_extract(col('input_file_name()'), './(links)([\w\.]+)',2))
  clean_name_df_subset = clean_name_df.select("Community")
  #establish id as the common key between the two dataframes created above
  tdf_id = tdf.withColumn("iid", monotonically_increasing_id())
  fdf_id = clean_name_df_subset.withColumn("iid", monotonically_increasing_id())
  community_links = tdf_id.join(fdf_id, tdf_id.iid == fdf_id.iid).drop("iid")
  community_links.write.parquet('s3a://xmlparq/links.parquet')
  spark.catalog.clearCache()
