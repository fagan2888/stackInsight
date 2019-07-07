#!/bin/bash
url=$1
#shell script to extract stackoverflow xml files
echo $url

echo "Unzipping the 7z file"
file_name=$(echo $url | rev | cut -d '/' -f 1 | rev)
just_file_name=$(echo $file_name | rev | cut -d '.' -f2- | cut -d '-' -f2- | rev)
file_type=$(echo $file_name | rev | cut -d '.' -f1 | rev)
echo $file_name
7za e $file_name

echo "Unzipping complete"
echo "rename xml files"

if [ "$file_type" == "Votes.7z" ]
then 
    mv Votes.xml "votes$just_file_name.xml"
fi

if [ "$file_type" == "Badges.7z" ]
then 
    mv Badges.xml "badges$just_file_name.xml"
fi

if [ "$file_type" == "Tags.7z" ]
then 
    mv Badges.xml "tags$just_file_name.xml"
fi

if [ "$file_type" == "Users.7z" ]
then 
    mv Users.xml "users$just_file_name.xml"
fi

if [ "$file_type" == "Posts.7z" ]
then 
    mv Posts.xml "postss$just_file_name.xml"
fi

if [ "$file_type" == "PostLinks.7z" ]
then 
    mv PostLinks.xml "links$just_file_name.xml"
fi

if [ "$file_type" == "PostHistory.7z" ]
then 
    mv PostLinks.xml "phist$just_file_name.xml"
fi

if [ "$file_type" == "Comments.7z" ]
then 
    mv Comments.xml "comments$just_file_name.xml"
fi

echo "rename complete"

echo "Delete the files"

rm -r *.7z
