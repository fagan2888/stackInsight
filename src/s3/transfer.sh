#!/bin/bash

# This script is used to redirect the archives in the urls of the csv file to the amazon s3 bucket

input=$HOME'/urls_list_output.csv'
echo $input
while read url
do
    

    echo $url
    echo "Transferring to S3"
    file_name=$(echo $url | rev | cut -d '/' -f 1 | rev)
    echo $file_name
    wget -qO- $url | aws s3 cp - s3://sedatadump/$file_name

    echo "Transfer Complete"
done < "$input"
