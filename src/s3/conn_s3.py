import os
import boto3

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID','default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

bucket_name = "sedatadump"

file_name= "android.stackexchange.com.7z"

s3 = boto3.client('s3')
s3.download(bucket_name,file_name,file_name)

#conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

#bucket = conn.get_bucket(bucket_name)
#key = bucket.get_key(filename)

#conn.close()

