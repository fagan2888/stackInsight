import boto3
import subprocess
import csv

bucket_name = "sedatadump"
destination_bucket_name = "xmlsefiles"

#python script to download the .7z files from s3 and convert them to xml files
file_path='/home/ubuntu/urls_subset_list.csv'
with open(file_path, 'r') as f:
  reader = csv.reader(f)
  urls_list = list(reader)


for urls in urls_list:
  #retrieve the filename from the stackexchange website url:
  url_name= urls[0]
  file_name= url_name.split("/")[-1]
  print(file_name)

   
  #generate the corresponding posts.gz and links.gz file names
  xml_file_name_posts = "posts" + file_name + "xml"
  xml_file_name_links = "links" + file_name + "xml"
  xml_file_name_badges = "badges" + file_name + "xml"
  xml_file_name_tags = "tags" + file_name + "xml"
  xml_file_name_votes = "votes" + file_name + "xml"
  xml_file_name_badges = "links" + file_name + "xml"
  xml_file_name_phist = "phist" + file_name + "xml"
  xml_file_name_comments = "comments" + file_name + "xml"


 
  #download the file name from the s3 bucket
  print('Stage 1: Pull data from S3 bucket')
  s3 = boto3.client('s3')
  s3.download_file(bucket_name,file_name,file_name)

  
  print('Stage 2: Shell Script to extract the xml files from the .7z files')
  #shell script to convert the .7z file to .gz files
  if "stackoverflow.com" in file_name:
    command = "/home/ubuntu/so.sh " + urls[0]
    process_output = subprocess.call([command],shell=True)
  else:  
    command = "/home/ubuntu/sop.sh " + urls[0]
    process_output = subprocess.call([command],shell=True)


  #upload the gz files to the amazon s3 bucket
  print('Stage 3: Data Load to s3 stage')
  ds3 = boto3.resource('s3')
  print('Loading to bucket ' + xml_file_name_posts)
  ds3.meta.client.upload_file(xml_file_name_posts, destination_bucket_name, xml_file_name_posts)
  print('Loading to bucket ' + xml_file_name_links)
  ds3.meta.client.upload_file(xml_file_name_links, destination_bucket_name, xml_file_name_links)
  print('Loading to bucket ' + xml_file_name_badges)
  ds3.meta.client.upload_file(xml_file_name_badges, destination_bucket_name, xml_file_name_badges)
  print('Loading to bucket ' + xml_file_name_tags)
  ds3.meta.client.upload_file(xml_file_name_tags, destination_bucket_name, xml_file_name_tags)
  print('Loading to bucket ' + xml_file_name_votes)
  ds3.meta.client.upload_file(xml_file_name_votes, destination_bucket_name, xml_file_name_votes)
  print('Loading to bucket ' + xml_file_name_badges)
  ds3.meta.client.upload_file(xml_file_name_badges, destination_bucket_name, xml_file_name_badges)
  print('Loading to bucket ' + xml_file_name_phist)
  ds3.meta.client.upload_file(xml_file_name_phist, destination_bucket_name, xml_file_name_phist)
  print('Loading to bucket ' + xml_file_name_comments)
  ds3.meta.client.upload_file(xml_file_name_comments, destination_bucket_name, xml_file_name_comments)
  
  print('Delete the xml files post upload')
  command = "rm *.xml"
  process_output2 = subprocess.call([command],shell=True)

