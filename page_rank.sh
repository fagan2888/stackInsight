# !/bin/bash

export PYSPARK_PYTHON=python3;
/usr/local/spark/bin/spark-submit \
   --master spark://ip-10-0-0-13:7077 \
   --deploy-mode cluster \
   --executor-memory 20G \
   --total-executor-cores 4 \
   --jars postgresql-9.4.1207.jar,aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar \
   --packages graphframes:graphframes:0.5.0-spark2.1-s_2.1 \
   /home/ubuntu/stackInsights/spark/pagerank_calculation.py
