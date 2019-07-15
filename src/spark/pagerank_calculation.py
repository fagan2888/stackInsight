""""
This piece of code calculates the page rank of each if the pages if they are linked by other pages
""""
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
import re,html,csv
from graphframes import *
#set up a spark session:

def main():
  sc = SparkContext(conf=SparkConf().setAppName("se"))
  spark = SparkSession.builder.appName("se").getOrCreate()
  df3 = spark.read.load("s3a://xmlparq/links.parquet")

  #find only those links which are not duplicate:
  links = df3.filter((f.col('LinkTypeId')==1))
  #Extract only those columns which are required for our analysis:
  partitioneddf = df3.repartition("Community")
  links_df_subset = partitioneddf.select('Id', 'CreationDate', 'RelatedPostId','Community')
  #Rename the columns 
  new_column_names = ['src', 'CreationDate' ,'dst','community']
  edges = links_df_subset.toDF(*new_column_names)
  v1 = links.select('Id','community','CreationDate')
  v2 = links.select('RelatedPostId','community','CreationDate')
  v1 = v1.select('Id','community').distinct()
  v2 = v2.select('RelatedPostId','community').distinct()
  v1 = v1.withColumnRenamed("Id","id")
  v2 = v2.withColumnRenamed("RelatedPostId","id")
  vertices = v1.union(v2)
  graph_frame = GraphFrame(vertices,e)
  #perform pagerank to identify pages with high rank
  pr = graph_frame.pageRank(resetProbability=0.15, tol=0.01)
  pr_df = pr.vertices.sort(['pagerank'],ascending=[0])
  pr_df = pr_df.withColumnRenamed("pagerank","cred_score")
  pr_df = pr_df.withColumn("cred_score", f.round(pr_df["cred_score"], 5))
  pr_df.write.parquet("s3a://xmlparq/pr_se_links.parquet")

  sc = SparkContext(conf=SparkConf().setAppName("se"))
  spark = SparkSession.builder.appName("se").getOrCreate()
  df3 = spark.read.load("s3a://xmlparq/links.parquet")

  #find only those links which are not duplicate:
  links = df3.filter((f.col('LinkTypeId')==1))
  #Extract only those columns which are required for our analysis:
  partitioneddf = df3.repartition("Community")
  links_df_subset = partitioneddf.select('Id', 'CreationDate', 'RelatedPostId','Community')
  #Rename the columns 
  new_column_names = ['src', 'CreationDate' ,'dst','community']
  edges = links_df_subset.toDF(*new_column_names)
  v1 = links.select('Id','community','CreationDate')
  v2 = links.select('RelatedPostId','community','CreationDate')
  v1 = v1.select('Id','community').distinct()
  v2 = v2.select('RelatedPostId','community').distinct()
  v1 = v1.withColumnRenamed("Id","id")
  v2 = v2.withColumnRenamed("RelatedPostId","id")
  vertices = v1.union(v2)
  graph_frame = GraphFrame(vertices,e)
  #perform pagerank to identify pages with high rank
  pr = graph_frame.pageRank(resetProbability=0.15, tol=0.01)
  pr_df = pr.vertices.sort(['pagerank'],ascending=[0])
  pr_df = pr_df.withColumnRenamed("pagerank","cred_score")
  pr_df = pr_df.withColumn("cred_score", f.round(pr_df["cred_score"], 5))
  pr_df.write.parquet("s3a://xmlparq/pr_se_links.parquet")

  

if __name__ == "__main__":
  
  main()
