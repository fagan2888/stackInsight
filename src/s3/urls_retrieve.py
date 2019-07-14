from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
import csv
import time

def write_to_csv(list_files,file_name):
   if len(list_files) >0:
      with open(file_name + '.csv', 'w') as target:
           writer = csv.writer(target)
           for row in urls_list:
             writer.writerow([row])
           

url = 'https://archive.org/download/stackexchange'
response = requests.get(url)
page_content = BeautifulSoup(response.text)
tabledatas = page_content.find_all("td")[1:]
urls_list = []
for tabledata in tabledatas:
  if len(tabledata.find_all("a"))>0 :
    if tabledata.find_all("a")[0]['href'][-3:] == '.7z':
      urls_list.append('https://archive.org/download/stackexchange/'  + tabledata.find_all("a")[0]['href'])


write_to_csv(urls_list,'/home/ubuntu/urls_list')
