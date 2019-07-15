# stackInsight
Insight Data Engineering Project New York 2019B Session

| ->  [Demo](https://www.datalit.info)        |                ->  [Slides](https://docs.google.com/presentation/d/1Vxph2p5KekOKe9e2O7LTa55-IDVIYwcrO0I3Z2CUGKI/edit?usp=sharing)           |
| ------------- |:-------------:|



## 1.Overview
The stackexchange network has over 170 QnA communites dedicated to answering questions about a variety of topics for people all over the world from different backgrounds. With so many questions being asked across different topics, it would certainly be interesting to see which topics have the most active users.
This would be useful in the following ways:
* Identify pages which topics have a low response time

* Place targeted ads on pages which have a high pagerank score in comparision to the rest of the pages.

* Compare different technologies and see how the metrics differ and set expectations on the basis of the metrics.

* Can be used by small startups which has launched a new product to monitor how their tech is performing in the community.


## 2. Pipeline
![diagram](fig/pipeline.png)

## 3. Requirements
- Python3
- [AWS CLI](https://aws.amazon.com/cli/)
- [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation)
## Architechture:

### Spark:

4 EC2 m5ad.xlarge instances (1 master 3 slaves spark cluster)

[Installation](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88)

### Airflow:

1 EC2 m5ad.xlarge instance

[Installation](https://blog.insightdatascience.com/scheduling-spark-jobs-with-airflow-4c66f3144660)

### PostgreSQL:

1 EC2 m5ad.xlarge instance

[Installation](https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252)

### Dash
1 EC2 m5ad.large instance

[Installation](https://dash.plot.ly/installation)

## DataSet
StackExchange data dump from the online internet archive [Link](https://archive.org/download/stackexchange) 

## Metrics
For a given online community and a set of tags entered by a user, the dashboard displays:
1. Total number of questions being asked over time.

2. The average response time(in Days) of the answers to the questions over time.

3. The proportion of questions which have received an acceptable answer over time.

4. The page rank of the question pages and how it varies over time

## Data Engineering Takeaways:

1. XML files were to converted to parquet files.

2. Spark was used to calculate the response time based on the timestamps of the questions and answers.

3. Graphframes were used from the GraphFrame library to calculate pagerank on the pages and the corresponding links to identify the important pages based on the number of incoming links to the pages.


## Dashboard
![diagram](fig/db_screenshot.png)
