#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
day=$1
fi


/home/hdp-voice/All_tantan_login.sh $day
