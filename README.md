# stackInsight
insight data engineering project
## Motivation
The stackexchange network has over 170 QnA communites dedicated to answering questions about a variety of topics for people all over the world from different backgrounds. It would certainly be intersting to analyse which product(read tags) are more popular and active. By calculation of different types of metrics , my deliverable is a dashboard which could be used to float targeted ads on the pages which have the relevant products.
"""

## Pipeline
![diagram](fig/pipeline.png)

## DataSet
StackExchange data dump from the online internet archive [Link](https://archive.org/download/stackexchange) 

### Metrics
For a given online community and a set of tags entered by a user, the dashboard displays:
1. Total number of questions being asked over time
2. The average response time of the answers to the questions over time.
3. The proportion of questions which have received an acceptable answer over time
4. The page rank of the question pages and how it varies over time
